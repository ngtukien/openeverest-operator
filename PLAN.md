# KẾ HOẠCH TÍCH HỢP CLOUDNATIVE-PG VÀO OPENEVEREST OPERATOR v1.16.2

Tài liệu này đặc tả chi tiết **Kiến trúc kỹ thuật**, **Danh mục công việc (Tasks)**, **Ánh xạ mã nguồn (Code Mapping)** và **Tiêu chí nghiệm thu (Acceptance Criteria)** cho từng Phase trong lộ trình tích hợp CloudNativePG (CNPG) làm một Provider CSDL PostgreSQL chính thức trên OpenEverest.

---

## 🏛️ TỔNG QUAN KIẾN TRÚC TÍCH HỢP (ARCHITECTURE OVERVIEW)

OpenEverest Operator đóng vai trò là **Meta-Operator (Lớp trừu tượng hóa DBaaS)**. Khi người dùng khai báo `DatabaseCluster` với `spec.engine.provider: cloudnative-pg`, Everest sẽ tự động điều phối và chuyển đổi (reconcile) thành các tài nguyên native của CloudNativePG Operator:

```text
       [ LẬP TRÌNH VIÊN / DEVOPS / REST API ]
                          │
                          │ kubectl apply (CRD DatabaseCluster)
                          ▼
      ┌────────────────────────────────────────────────────────┐
      │               OpenEverest Operator                     │
      │   - Controller: databasecluster_controller.go          │
      │   - Adapter:    providers/cnpg (Provider & Applier)    │
      └───────────────────────────┬────────────────────────────┘
                                  │
                   Sinh CRD Native của CloudNativePG:
                   • Cluster (postgresql.cnpg.io/v1)
                   • Backup & ScheduledBackup
                   • Managed Services (RW Endpoint)
                                  ▼
      ┌────────────────────────────────────────────────────────┐
      │             CloudNativePG Operator                     │
      │             (cnpg-controller-manager)                  │
      └───────────────────────────┬────────────────────────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        ▼                         ▼                         ▼
 ┌──────────────┐          ┌──────────────┐          ┌──────────────┐
 │ Pods (CNPG)  │          │ K8s Services │          │ Barman S3    │
 │ Direct Pod   │          │ <cluster>-rw │          │ Object Store │
 │ Management   │          │ (LoadBalancer│          │ (SeaweedFS / │
 │ + PVC riêng  │          │  /ClusterIP) │          │  MinIO / AWS)│
 └──────────────┘          └──────────────┘          └──────────────┘
```

### Nguyên tắc tương thích cốt lõi:
1. **Zero Breaking Changes:** Khi `spec.engine.provider` để trống, Everest mặc định 100% sử dụng `percona-postgresql` như cũ.
2. **Tính bất biến (Immutability):** Provider không thể chuyển đổi giữa chừng trên một cụm CSDL đang chạy.
3. **Decoupled Architecture:** Everest Operator tương tác với CNPG qua **Dynamic Unstructured Client**, không bị phụ thuộc tĩnh (hard compile-time dependency) vào Go package của CNPG.

---

## 📅 CHI TIẾT CÔNG VIỆC VÀ KIẾN TRÚC TỪNG PHASE

---

### Phase 1 — Provider API, Schema & Dynamic Discovery
* **Trạng thái:** ✅ **ĐÃ HOÀN THÀNH (100% CODE)**

#### 1. Kiến trúc kỹ thuật:
Mở rộng API Schema của Everest để hỗ trợ thêm trường `provider`. Để tránh lỗi khởi động Operator trên các cụm K8s chưa cài đặt CRD của CNPG, cơ chế **Dynamic Discovery** được áp dụng: Everest chỉ đăng ký Watcher cho CNPG khi CRD `clusters.postgresql.cnpg.io` đã tồn tại trên K8s API server.

```text
[ReconcileWatchers] ─── Kiểm tra API Server ───► Có CRD clusters.postgresql.cnpg.io?
                                                      │
                                                      ├─ CÓ ──► Đăng ký Watcher Unstructured CNPG
                                                      └─ KHÔNG ► Bỏ qua an toàn (IsNotFound)
```

#### 2. Danh mục công việc & Files liên quan:
- [`api/everest/v1alpha1/databasecluster_types.go`](file:///home/ngtukien/Projects/DBaaS/operator/api/everest/v1alpha1/databasecluster_types.go):
  - Thêm trường `spec.engine.provider` (`cloudnative-pg` | `percona-postgresql`).
  - Thêm enum `DatabaseEngineProvider` và hàm fallback `EffectiveProvider()`.
  - Thêm validation rule: chỉ `engine.type: postgresql` mới được khai báo `provider`.
- [`internal/consts/consts.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/consts/consts.go):
  - Khai báo các hằng số: `CNPGAPIGroup`, `CNPGClusterKind`, `CNPGDeploymentName`, `CNPGOperatorNamespace`.
- [`internal/controller/everest/databasecluster_controller.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/controller/everest/databasecluster_controller.go):
  - `ReconcileWatchers()`: Khởi tạo dynamic watcher cho đối tượng CNPG Cluster.

#### 3. Tiêu chí nghiệm thu (Acceptance Criteria):
- Tạo `DatabaseCluster` không có provider $\rightarrow$ Tự động gán `percona-postgresql`.
- Tạo `DatabaseCluster` với `provider: cloudnative-pg` $\rightarrow$ Nhận diện chính xác và không sinh CR của Percona.
- Operator khởi động bình thường kể cả khi cụm K8s chưa cài đặt CRD của CNPG.

---

### Phase 2 — CNPG Database Provider Adapter (Core Translation)
* **Trạng thái:** ✅ **ĐÃ HOÀN THÀNH (100% CODE)**

#### 1. Kiến trúc kỹ thuật:
Hiện thực hóa Adapter Pattern qua interface `providers.Provider` và `everestv1alpha1.Applier`. Adapter nhận dữ liệu từ `DatabaseClusterSpec` và biên dịch thành `spec` của CNPG `Cluster` (`postgresql.cnpg.io/v1`).

```text
Everest DatabaseCluster                         CloudNativePG Cluster
├── engine.version       ───────────────►       ├── spec.imageName (ghcr.io/...)
├── engine.replicas      ───────────────►       ├── spec.instances
├── engine.resources     ───────────────►       ├── spec.resources (requests/limits)
├── engine.storage       ───────────────►       ├── spec.storage (size, storageClass)
├── engine.config        ───────────────►       ├── spec.postgresql.parameters
├── engine.userSecret    ───────────────►       ├── spec.bootstrap.initdb.secret
└── proxy.expose         ───────────────►       └── spec.managed.services.additional
```

#### 2. Danh mục công việc & Files liên quan:
- [`internal/controller/everest/providers/cnpg/provider.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/controller/everest/providers/cnpg/provider.go):
  - Struct `Provider` nhúng `*unstructured.Unstructured` để thao tác dynamic với CNPG.
  - Hàm `New()`: Khởi tạo instance và resolve image URL PostgreSQL.
- [`internal/controller/everest/providers/cnpg/applier.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/controller/everest/providers/cnpg/applier.go):
  - `Engine()`: Ánh xạ cấu hình phần cứng, storage, initdb secret và parse chuỗi cấu hình `postgresql.conf`.
  - `Proxy()`: Cấu hình Service RW (LoadBalancer/NodePort) trong `spec.managed.services.additional`.

#### 3. Tiêu chí nghiệm thu:
- Khởi tạo cụm CNPG 3 node với đầy đủ tài nguyên CPU, RAM, StorageClass và tham số `postgresql.conf`.
- Endpoint kết nối được mở đúng kiểu mạng (`ClusterIP` hoặc `LoadBalancer`).

---

### Phase 3 — Status Mapping, Ownership & Zero-Downtime Failover
* **Trạng thái:** ✅ **CODE ĐÃ XONG (100%)** — ⚠️ *Chờ E2E Test Lab*

#### 1. Kiến trúc kỹ thuật:
- **Status Loop:** Adapter theo dõi các Condition của CNPG. Khi condition `Ready == True` và số `readyInstances == desired` $\rightarrow$ Everest chuyển trạng thái sang `AppStateReady`.
- **Ownership:** Thiết lập `OwnerReference` từ `DatabaseCluster` sang CNPG `Cluster` để Kubernetes Garbage Collection tự dọn dẹp khi xóa cụm.
- **Failover Contract:** Everest chỉ công bố 1 endpoint duy nhất trỏ vào Service Read-Write (`<cluster>-rw.<ns>.svc`). Khi node Primary gặp sự cố, CNPG tự động chuyển backend của Service sang Primary mới mà không làm thay đổi DNS/Hostname của Everest.

```text
[Ứng dụng kết nối] ──► Hostname cố định: <cluster>-rw.<ns>.svc
                                │
          ┌─────────────────────┴─────────────────────┐
          │ Khi failover: Service tự đổi Pod backend  │
          ▼                                           ▼
  [ Node 1: Primary (Lỗi) ]                   [ Node 2: Primary Mới ]
```

#### 2. Danh mục công việc & Files liên quan:
- [`internal/controller/everest/providers/cnpg/provider.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/controller/everest/providers/cnpg/provider.go):
  - `Status()`: Chuẩn hóa port (5432), hostname, size, ready và parse chi tiết conditions.
  - `Cleanup()`: Dọn dẹp an toàn các bản backup/restore trước khi gỡ finalizer.
- [`internal/controller/everest/databasecluster_controller.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/controller/everest/databasecluster_controller.go):
  - Bắt sự kiện cập nhật trạng thái từ CNPG Cluster để trigger reconcile ngay lập tức.

#### 3. Tiêu chí nghiệm thu:
- Kill Pod Primary $\rightarrow$ CNPG bầu chọn Standby lên làm Primary mới.
- Hostname/IP LoadBalancer không thay đổi.
- Ứng dụng tự reconnect và ghi dữ liệu mới thành công sau failover.

---

### Phase 4 — Backup, ScheduledBackup & Phục hồi dữ liệu (Restore/PITR)
* **Trạng thái:** ✅ **CODE ĐÃ XONG (100%)** — ⚠️ *Chờ E2E Test Lab*

#### 1. Kiến trúc kỹ thuật:
- **Barman Object Store:** Chuyển đổi `BackupStorage` của Everest (S3/SeaweedFS/MinIO/Azure) thành cấu hình `spec.backup.barmanObjectStore` của CNPG.
- **On-Demand & Scheduled Backup:** Ánh xạ `DatabaseClusterBackup` sang `Backup.postgresql.cnpg.io/v1` và `spec.backup.schedules` sang `ScheduledBackup.postgresql.cnpg.io/v1`.
- **Restore & PITR (Blue-Green Restore):** Do `spec.bootstrap` của CNPG là bất biến, việc phục hồi được thực hiện bằng cách bootstrap một cụm DatabaseCluster mới từ `spec.dataSource`, kết nối vào Barman Object Store để tải Base Backup và replay WAL đến mốc `targetTime` (nếu dùng PITR).

```text
DatabaseClusterBackup (Everest)  ────────►  Backup (CNPG CRD)
                                                 │
                                                 ▼ Đẩy dữ liệu qua Barman
                                            [ SeaweedFS S3 Bucket ]
                                                 ▲ Kéo WAL / Snapshot
                                                 │
DatabaseCluster (Mới) DataSource ────────►  Cluster.spec.bootstrap.recovery
```

#### 2. Danh mục công việc & Files liên quan:
- [`internal/controller/everest/databaseclusterbackup_controller.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/controller/everest/databaseclusterbackup_controller.go):
  - `tryCreateCNPG()`: Bắt sự kiện backup từ CNPG ScheduledBackup để tự tạo `DatabaseClusterBackup`.
  - `reconcileCNPG()`: Xóa an toàn 2 pha (xóa upstream CNPG Backup trước, gỡ finalizer sau).
  - `getCNPGBackupStatus()`: Chuyển đổi trạng thái phase và timestamps RFC3339Nano.
- [`internal/controller/everest/databaseclusterrestore_controller.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/controller/everest/databaseclusterrestore_controller.go):
  - `restoreCNPG()`: Xác thực bootstrap từ `spec.dataSource`.
  - `reconcileStatus()`: Chuyển trạng thái sang `RestoreSucceeded` khi cụm mới Ready.
- [`internal/controller/everest/providers/cnpg/backup.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/controller/everest/providers/cnpg/backup.go):
  - `BarmanObjectStore()`: Ánh xạ S3 credentials, endpointURL, bucket.

#### 3. Tiêu chí nghiệm thu:
- Tạo backup thủ công và chạy backup theo lịch cron đẩy thành công lên S3/SeaweedFS.
- Dựng cụm mới từ bản backup hoặc Point-in-Time Recovery thành công, dữ liệu toàn vẹn.

---

### Phase 5 — Database Lifecycle, Scheduling & Online PVC Expansion
* **Trạng thái:** 🚀 **ĐANG THỰC HIỆN (ACTIVE)**

#### 1. Kiến trúc kỹ thuật:
- **Scale ngang (Instances):** Thay đổi `engine.replicas` $\rightarrow$ CNPG tự động tạo Pod mới và clone dữ liệu qua `pg_basebackup` mà không gây downtime cho Primary.
- **Online PVC Expansion:** Mở rộng dung lượng `storage.size` trực tiếp. CSI Driver mở rộng filesystem online. Bổ sung quan sát trạng thái **`AppStateResizingVolumes`** trong lúc đĩa đang resize.
- **Pod Scheduling & Anti-Affinity:** Chuyển tiếp `PodSchedulingPolicy` của Everest thành `spec.affinity` của CNPG, cấu hình rải Pod giữa các Availability Zones (`topology.kubernetes.io/zone`).
- **Minor Version Rolling Upgrade:** Nâng cấp minor image (ví dụ: `16.1` lên `16.4`), CNPG tự động thực hiện rolling update từng Standby trước rồi switchover Primary để giảm thiểu downtime tối đa (1–2 giây).

#### 2. Danh mục công việc & Files liên quan:
- Cập nhật [`providers/cnpg/provider.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/controller/everest/providers/cnpg/provider.go):
  - Bổ sung hàm kiểm tra PVC resize để cập nhật `status.status = everestv1alpha1.AppStateResizingVolumes`.
- Cập nhật [`providers/cnpg/applier.go`](file:///home/ngtukien/Projects/DBaaS/operator/internal/controller/everest/providers/cnpg/applier.go):
  - Hoàn thiện chuyển tiếp `PodSchedulingPolicy` (NodeAffinity, PodAntiAffinity, Tolerations).
  - Đảm bảo cơ chế kiểm tra tính hợp lệ khi nâng cấp version image.

#### 3. Tiêu chí nghiệm thu:
- Tăng số node từ 3 lên 5: Cụm scale thành công, không downtime.
- Tăng ổ đĩa từ 10Gi lên 20Gi: Everest hiển thị `resizingVolumes`, sau đó trở lại `ready` mà không restart DB.
- Pods được phân bổ đều trên các Node/Zone khác nhau theo policy.

---

### Phase 6 — Validating Webhook & Capability Guard
* **Trạng thái:** 📋 **KẾ HOẠCH**

#### 1. Kiến trúc kỹ thuật:
Sử dụng Kubernetes Validating Admission Webhook để chặn các cấu hình không hợp lệ ngay tại thời điểm `kubectl apply`, bảo vệ hệ thống khỏi lỗi runtime.

```text
[kubectl apply] ──► [K8s API Server] ──► [Validating Webhook]
                                                │
                                                ├─ Hợp lệ ────► Ghi vào etcd
                                                └─ Không hợp lệ ► Reject ngay lập tức (HTTP 400)
```

#### 2. Danh mục công việc:
- Chặn thay đổi `spec.engine.provider` sau khi tạo (Immutable check).
- Chặn khai báo `proxy.type` hoặc `proxy.replicas` khi dùng CNPG (chỉ cho phép `proxy.expose`).
- Kiểm tra sự tồn tại của CRD CNPG trước khi cho phép tạo cluster dạng `cloudnative-pg`.
- Chặn các tính năng chưa hỗ trợ (PMM Monitoring, Data Import).

#### 3. Tiêu chí nghiệm thu:
- Sửa provider trên cluster đang chạy $\rightarrow$ Bị Webhook từ chối ngay lập tức.
- Cấu hình proxy phức tạp với CNPG $\rightarrow$ Báo lỗi rõ ràng hướng dẫn chỉ dùng `proxy.expose`.

---

### Phase 7 — Phân quyền RBAC & Đóng gói OLM Bundle
* **Trạng thái:** 📋 **KẾ HOẠCH**

#### 1. Kiến trúc kỹ thuật:
Cập nhật ClusterRole của Everest Operator tuân thủ nguyên tắc đặc quyền tối thiểu (Principle of Least Privilege), chỉ cấp quyền trên đúng API group `postgresql.cnpg.io`. Đóng gói Operator Lifecycle Manager (OLM) CSV/Bundle.

#### 2. Danh mục công việc:
- Cập nhật [`config/rbac/role.yaml`](file:///home/ngtukien/Projects/DBaaS/operator/config/rbac/role.yaml): Cấp quyền `get, list, watch, create, update, patch, delete` cho `clusters`, `backups`, `scheduledbackups`.
- Cập nhật OLM ClusterServiceVersion (CSV) và `deploy/bundle.yaml`.

#### 3. Tiêu chí nghiệm thu:
- Everest Operator chạy bình thường với ServiceAccount mặc định, không gặp lỗi `403 Forbidden` khi thao tác tài nguyên CNPG.

---

### Phase 8 — Giám sát & Vận hành (Observability)
* **Trạng thái:** 📋 **KẾ HOẠCH**

#### 1. Kiến trúc kỹ thuật:
Tận dụng Metrics Exporter tích hợp sẵn của CloudNativePG (cổng 9187 trên từng Pod) để xuất metric Prometheus chuẩn (`cnpg_collector_*`), tích hợp vào hệ thống giám sát của Everest/Grafana.

#### 2. Danh mục công việc:
- Khởi tạo `PodMonitor` hoặc cấu hình Prometheus scrape metrics từ CNPG pods.
- Giám sát các chỉ số cốt lõi: Replication lag, WAL archiving rate, CPU/RAM saturation, Connection count.
- Xây dựng AlertManager rules: Cảnh báo failover, mất node Standby, backup lỗi.

#### 3. Tiêu chí nghiệm thu:
- Dashboard Grafana hiển thị đầy đủ thông số sức khỏe và hiệu năng của cụm CNPG.

---

### Phase 9 — Kiểm thử nghiệm thu tổng thể (End-to-End Acceptance)
* **Trạng thái:** 📋 **KẾ HOẠCH**

#### 1. Kịch bản kiểm thử:
1. **Provisioning:** Dựng cụm 1 node và 3 node HA qua Everest manifest.
2. **Scale & Resize:** Scale ngang replicas, mở rộng đĩa online.
3. **Failover:** Kill node Primary, đo lường RTO (< 10s) và RPO (0s), xác nhận kết nối tự phục hồi.
4. **Backup & PITR:** Chụp backup lên S3, xóa bảng dữ liệu, tua ngược thời gian khôi phục thành công.

---

### Phase 10 — CNPG Logical Replication (Publication & Subscription)
* **Trạng thái:** 📋 **KẾ HOẠCH**

#### 1. Kiến trúc kỹ thuật:
Phục vụ nhu cầu đồng bộ dữ liệu giữa các microservices hoặc giữa các cụm CSDL khác nhau mà không cần đồng bộ toàn bộ đĩa vật lý.

```text
[ Cụm CNPG Nguồn (A) ]                           [ Cụm CNPG Đích (B) ]
 └── Publication.postgresql.cnpg.io  ──Logical──►  └── Subscription.postgresql.cnpg.io
```

#### 2. Danh mục công việc:
- Quản lý CRD `Publication` và `Subscription` của CNPG qua Everest.
- Đồng bộ Sequence trước cutover và kiểm soát schema DDL gap.

---

### Phase 11 — CNPG Replica Cluster (Cross-Zone / Cross-Cluster HA & DR qua pg_basebackup)
* **Trạng thái:** 📋 **KẾ HOẠCH**

#### 1. Kiến trúc kỹ thuật:
Bảo vệ hệ thống ở cấp độ thảm họa trung tâm dữ liệu (Datacenter / Zone failure). Dựng một cụm bản sao (Replica Cluster) ở Zone B hoặc Kubernetes Cluster khác:

```text
[ ZONE A: PRIMARY CLUSTER ]                       [ ZONE B: REPLICA CLUSTER ]
   cnpg-db-primary (RW)                             cnpg-db-replica (Standby Cluster)
          │                                                │
          ├─────── 1. Bootstrap: pg_basebackup ───────────►│ (Clone dữ liệu ban đầu)
          │                                                │
          └─────── 2. Continuous Physical Streaming ──────►│ (Replay WAL liên tục)
                                                           │
          [ Sự cố Zone A! ]                                │
          ┌────────────────────────────────────────────────┘
          ▼
   3. Gạt cờ spec.replica.enabled: false
      ──► Tự động Promote thành Primary mới nhận ghi!
```

#### 2. Danh mục công việc:
- Mở rộng CRD `DatabaseCluster`: thêm cấu hình `spec.replica`.
- Adapter `applier.go`: Render `spec.replica.enabled: true`, `spec.bootstrap.pg_basebackup.source` và `spec.externalClusters`.
- Bổ sung đo lường byte lag (`pg_wal_lsn_diff`) giữa 2 cụm vào `DatabaseCluster.status`.

#### 3. Tiêu chí nghiệm thu:
- Cụm Zone B clone snapshot thành công qua `pg_basebackup` từ Zone A qua mạng.
- Ghi dữ liệu ở Zone A xuất hiện tức thì ở Zone B.
- Diễn tập thăng cấp (DR Promotion): Ngắt Zone A, promote cụm Zone B nhận ghi thành công trong vài giây.

---

## 🏁 ĐIỀU KIỆN HOÀN TẤT DỰ ÁN (DEFINITION OF DONE)

1. **Bảo toàn tính nguyên bản:** Cụm Percona PostgreSQL cũ hoạt động bình thường, không thay đổi bất kỳ hành vi nào khi không chọn provider.
2. **Provider độc lập:** CNPG hoạt động độc lập, không sinh nhầm tài nguyên của Percona.
3. **Tính sẵn sàng cao:** Chịu lỗi failover tự động trong cụm (< 10s) và hỗ trợ Disaster Recovery liên Zone.
4. **Vòng đời CSDL hoàn chỉnh:** Hỗ trợ đầy đủ Cấp phát, Mở rộng ổ đĩa online, Nâng cấp rolling, Sao lưu S3 và Phục hồi PITR.
5. **Mã nguồn chuẩn mực:** 100% các đoạn code tùy biến được gắn comment rõ ràng `// [CUSTOM CNPG]`, pass toàn bộ unit tests và linter.
