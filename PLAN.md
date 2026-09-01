# Plan bổ sung CloudNativePG cho Everest Operator v1.16.2

## 1. Mục tiêu và nguyên tắc tương thích

Mục tiêu là bổ sung CloudNativePG (CNPG) làm một PostgreSQL provider của Everest,
không thay thế hoặc xóa Percona PostgreSQL hiện có.

```yaml
spec:
  engine:
    type: postgresql
    provider: cloudnative-pg
```

Quy tắc tương thích:

- PostgreSQL không khai báo `spec.engine.provider` tiếp tục dùng `percona-postgresql`.
- Provider là immutable; không chuyển trực tiếp cluster đang chạy giữa Percona và CNPG.
- Everest sở hữu native database CR; database operator sở hữu Pod, PVC, Service và tài nguyên runtime bên dưới.
- Capability chưa hỗ trợ phải bị từ chối rõ ràng, không silently ignore hoặc tạo nhầm CR của provider còn lại.

## 2. Quyết định endpoint, proxy và luồng đọc/ghi

CNPG không dùng Patroni. Primary election và failover do CNPG operator, Instance
Manager và Kubernetes Lease xử lý.

MVP không triển khai HAProxy, PgBouncer/CNPG `Pooler`, hoặc read/write splitting:

```text
Client
  │ reconnect/retry
  ▼
Everest hostname: <cluster>-rw hoặc <cluster>-rw-external
  │ selectorType: rw
  ▼
CNPG primary hiện tại
```

Contract endpoint:

- Everest chỉ công bố một endpoint read-write trong `DatabaseCluster.status.hostname`.
- CNPG tự chuyển backend của service RW sang primary mới sau failover; DNS/LB endpoint không đổi.
- Connection đang mở có thể bị ngắt, nên client phải reconnect/retry.
- Không công bố service `-ro`/`-r` qua Everest API để tránh stale reads, read-after-write không nhất quán và routing nhầm replica đang lag.
- Với CNPG, chỉ `spec.proxy.expose` được tái sử dụng để cấu hình Service exposure. Các trường tạo proxy thực (`type`, `replicas`, `config`, `storage`, `resources`) là unsupported.
- Không có PgBouncer thì phải sizing `max_connections`, timeout và application-side pooling/retry để tránh connection storm sau failover.
- DR giữa hai CNPG Cluster không tự dùng chung endpoint; chuyển A sang B cần fencing, promotion và DNS/LB automation riêng.

## Phase 1 — Provider API, CNPG registration và compatibility

Trạng thái: **đã implement ở mức code, chờ cluster acceptance**.

- Thêm `spec.engine.provider` với `percona-postgresql` và `cloudnative-pg`.
- Empty provider mặc định về Percona để giữ backward compatibility.
- Thêm CNPG API group/kind constants và RBAC cho `Cluster`.
- Giữ nguyên Percona discovery, version service, scheme, RBAC và provider implementation.
- Chỉ đăng ký CNPG watcher khi CRD `clusters.postgresql.cnpg.io` tồn tại.
- Đồng bộ CRD, bundle và CSV packaging.

## Phase 2 — CNPG database provider

Trạng thái: **đã implement lõi ở mức code, chờ cluster acceptance**.

| Everest | CloudNativePG |
|---|---|
| `engine.version` | `spec.imageName` |
| `engine.replicas` | `spec.instances` |
| CPU/RAM | `spec.resources` |
| storage size/class | `spec.storage` |
| PostgreSQL config | `spec.postgresql.parameters` |
| scheduling policy | `spec.affinity` |
| user secret | `spec.bootstrap.initdb.secret` |
| `proxy.expose` | RW managed Service |

Provider hỗ trợ create, update và delete. Pause, PMM, backup, restore và import
CNPG hiện trả lỗi explicit.

## Phase 3 — Status, ownership và endpoint failover

Trạng thái: **đã implement cơ bản ở mức code, chờ E2E**.

- CNPG `Ready=True` và đủ `readyInstances` → Everest `Ready`.
- Map desired/ready instance count, port `5432` và status details.
- `status.hostname` luôn là endpoint RW, không trả endpoint replica.
- Owner reference: Everest `DatabaseCluster` → CNPG `Cluster`.
- Watch CNPG Cluster status khi CRD được cài.
- Cleanup finalizer không xóa nhầm backup/PVC và không ảnh hưởng Percona.

Acceptance bắt buộc:

1. Kill primary và chờ CNPG promote standby.
2. Xác nhận hostname/LB IP không đổi.
3. Xác nhận endpoint chuyển tới primary mới.
4. Xác nhận write mới thành công sau reconnect.
5. Không dùng `-ro` trong status hoặc test client.

## Phase 4 — Backup, ScheduledBackup và restore

Trạng thái: **đã implement lõi ở mức code, chờ E2E với CNPG/Barman object store**.

- `DatabaseClusterBackup` CNPG → `Backup.postgresql.cnpg.io`.
- Backup schedules → `ScheduledBackup.postgresql.cnpg.io`.
- Map `BackupStorage` sang Barman object store/plugin configuration.
- Map pending/running/completed/failed và completion time về Everest status.
- Full restore bằng `Cluster.spec.bootstrap.recovery`; hỗ trợ recovery target theo thời gian sau khi full restore ổn định.
- Restore CNPG chỉ dùng khi bootstrap cluster mới; không tự động xóa/recreate cluster đang chạy để giả lập restore in-place.
- Tách dispatch theo provider để CNPG không tạo Percona backup/restore.

## Phase 5 — Database lifecycle và scheduling

- Declarative database/user management bằng CNPG `Database` khi phù hợp.
- Scale instances, anti-affinity và topology.
- PVC expansion và trạng thái resize.
- PostgreSQL minor image update và rollback guard.
- PodSchedulingPolicy → CNPG affinity/tolerations/topology.
- Quy định rõ các bootstrap field immutable.

Không bao gồm CNPG `Pooler`, HAProxy hoặc read/write splitting.

## Phase 6 — Webhook và capability model

- Validate provider chỉ áp dụng cho PostgreSQL và không thể thay đổi.
- Validate CNPG CRD/operator tồn tại khi chọn `cloudnative-pg`.
- Validate image/version, replicas, storage và resources.
- Với CNPG, reject proxy `type`, `replicas`, `config`, `storage`, `resources`; chỉ cho phép `proxy.expose`.
- Reject PMM, pause hoặc import tới khi phase tương ứng hoàn tất; validate giới hạn backup/restore riêng của CNPG.
- Restore/backup source và target phải cùng provider.

## Phase 7 — RBAC, packaging và installation

- Quyền CNPG liệt kê theo resource/verb, không dùng wildcard.
- Everest khởi động bình thường nếu CNPG CRD chưa được cài.
- CNPG operator được cài độc lập; Everest chỉ consume CRD/API.
- Bundle, CSV, CRD và generated RBAC được regenerate cùng source markers.
- Không xóa Percona scheme, RBAC, controller hoặc dependencies.

## Phase 8 — Monitoring và vận hành

- Quan sát Cluster conditions, instance health, WAL/replication lag, PVC capacity và backup health.
- Mapping `MonitoringConfig` theo capability CNPG; không giả lập PMM.
- Alert failover, replica unhealthy, WAL archive failure, disk pressure và connection saturation.
- Runbook reconnect storm, node/storage loss và operator outage.

## Phase 9 — E2E, migration gate và production acceptance

- Provision 1 và 3 instances qua Everest.
- Scale, resize, image update và safe deletion.
- Fail primary, giữ nguyên RW endpoint và write lại sau reconnect.
- Backup/restore/PITR verification khi Phase 4 hoàn tất.
- Không có endpoint RO trong DBaaS response hoặc application configuration.
- `kubectl apply --dry-run=server`, `kubectl diff`, schema validation và full Go tests phải pass.
- Rollback bằng image/bundle revision; không xóa namespace/PVC để rollback.

## Phase 10 — CNPG Publication/Subscription

Mục tiêu là quản lý logical replication qua Everest thay vì apply CNPG manifest thủ công.

API và controller:

- Bổ sung Everest resource/API riêng cho Publication và Subscription; không nhồi logical replication vào `DatabaseCluster.spec.proxy`.
- Map sang `Publication.postgresql.cnpg.io/v1` và `Subscription.postgresql.cnpg.io/v1`.
- Thêm GVK/scheme hoặc unstructured type, watcher và RBAC tối thiểu cho `publications`, `subscriptions` và finalizers cần thiết.
- Owner reference và deletion policy phải tránh xóa publication/slot ngoài ý muốn.
- Map conditions, applied state, message và replication health về Everest status.

Validation và an toàn dữ liệu:

- Source/target database, publication, external cluster và credential Secret phải tồn tại.
- Secret chỉ được reference, không copy credential vào status/log.
- DDL không được logical replication tự đồng bộ; migration workflow có schema gate riêng.
- Đồng bộ sequence trước cutover.
- Kiểm tra replication lag và subscription errors trước khi đổi endpoint.
- Với PostgreSQL 16 trở xuống, kiểm thử logical slot survival qua HA failover.
- Cutover chỉ chuyển application sang endpoint RW của target CNPG Cluster; không dùng endpoint replica.

Acceptance Phase 10:

1. Everest tạo/xóa Publication và Subscription đúng provider.
2. Initial copy và DML replication thành công.
3. DDL gap được phát hiện và báo rõ.
4. Sequence sync hoàn tất trước cutover.
5. Fail primary target trong lúc replicate và xác nhận subscription recovery.
6. Cutover sang cùng một RW endpoint, không bổ sung read routing.

## Definition of Done tổng thể

- Percona PostgreSQL cũ không đổi hành vi khi provider rỗng.
- CNPG là opt-in và không tạo Percona PostgreSQL CR.
- Everest provision, observe và delete CNPG Cluster an toàn.
- Một endpoint RW ổn định qua failover nội cluster; client reconnect thành công.
- Không triển khai proxy/Pooler hoặc expose read-replica endpoint trong scope này.
- Backup/restore/PITR và Pub/Sub chỉ bật sau khi phase tương ứng pass E2E.
- Generated CRD/RBAC/bundle nhất quán và full test suite pass.
