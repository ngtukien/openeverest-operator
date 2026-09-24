# CloudNativePG provider (`engine.type: cnpg`)

Provider CNPG theo nguyên tắc 3 file của công ty, thao tác CR của CNPG qua `unstructured` nên không
phụ thuộc Go module của CNPG:

| File | Nội dung |
|---|---|
| `provider.go` | `Provider`, `Status`, `Cleanup`; phát hiện CRD, đọc `ClusterImageCatalog` (`EngineStatus`); `ValidateCreate`/`ValidateUpdate` cho webhook; RBAC marker |
| `applier.go` | 12 bước `Applier`, gồm pooler (`Proxy`), plugin backup (`Backup`), restore (`DataSource`) |
| `action.go` | Thao tác controller gọi vào: backup on-demand, nhận Backup từ lịch, status backup/restore, ObjectStore dùng chung |

Controller, webhook và API chỉ có các dòng `case` gọi vào package này; API `DatabaseCluster` chỉ
thêm `cnpg` vào enum `engine.type`.

## Điều kiện cài đặt

| Thành phần | Bắt buộc | Everest phát hiện bằng |
|---|---|---|
| CloudNativePG operator | Có | CRD `clusters.postgresql.cnpg.io` |
| `ClusterImageCatalog` tên `everest-postgresql` | Có | Danh sách version = các image trong catalog |
| Barman Cloud Plugin | Khi dùng backup | CRD `objectstores.barmancloud.cnpg.io` |
| `ClusterImageCatalog` tên `timescaledb-oss` | Khi dùng TimescaleDB | — |
| Prometheus Operator | Không | CRD `podmonitors.monitoring.coreos.com` → bật `enablePodMonitor` |

Thiếu CNPG hoặc catalog thì `DatabaseEngine cnpg-controller-manager` có danh sách version rỗng
và webhook từ chối mọi version (fail closed).

## Hợp đồng đầu vào

Field upstream của `DatabaseCluster`:

| Field | Ánh xạ sang CNPG |
|---|---|
| `engine.version` | `imageName` lấy từ catalog (pin digest); chỉ cho nâng minor |
| `engine.replicas`, `storage`, `resources` | `instances`, `storage`, `resources` |
| `engine.config` | `postgresql.parameters` (ghi đè mặc định nền tảng) |
| `engine.userSecretsName` | Secret có `username`, `password`, tuỳ chọn `database` (mặc định `app`) |
| `proxy.type: pgbouncer`, `replicas`, `resources`, `config: pool_mode = session\|transaction` | `Pooler` rw, thêm ro khi `replicas >= 2`, kèm PDB |
| `proxy.expose` | Service của pooler, hoặc `<name>-rw-external` khi không có pooler |
| `backup.schedules[]`, `backup.pitr` | Barman Cloud Plugin + `ScheduledBackup`; một `BackupStorage` cho mỗi cụm |
| `dataSource` | `bootstrap.recovery` qua plugin (restore chỉ khi tạo cụm mới) |
| `podSchedulingPolicyName` | `affinity`; dùng policy có `engineType: postgresql` |

Annotation (chỉ cho thứ API upstream không diễn đạt được), mỗi annotation một giá trị vô hướng,
tiền tố `cnpg.everest.io/`, key dạng `<loại>.<tên>.<trường>`. Key lạ bị từ chối.

| Annotation | Giá trị | Kết quả trên CNPG |
|---|---|---|
| `extension` | `timescaledb` (bất biến) | Catalog `timescaledb-oss`, `shared_preload_libraries` |
| `server-alt-dns-names` | DNS, phân tách bằng dấu phẩy | `certificates.serverAltDNSNames` |
| `source.<src>.host`, `.port`, `.dbname`, `.user`, `.sslmode` | vô hướng | Một PostgreSQL bên ngoài → `externalClusters` tên `source-<src>` |
| `source.<src>.password-secret` | `secret` hoặc `secret/key` (key mặc định `password`) | Xác thực bằng password |
| `source.<src>.cluster` | `name` hoặc `namespace/name` | Cụm CNPG nguồn: host `<name>-rw.<ns>.svc`, mặc định xác thực bằng cert `<name>-replication`/`<name>-ca` |
| `source.<src>.client-cert-secret`, `.ca-secret` | tên Secret | Ghi đè cert |
| `database.<db>.owner` | role, rỗng = owner trong user Secret | CR `Database`, `databaseReclaimPolicy: delete` |
| `publication.<pub>.dbname`, `.tables` | `.tables` là `*` (mặc định) hoặc `schema.table,...` | CR `Publication` |
| `subscription.<sub>.dbname`, `.publication`, `.source`, `.reclaim-policy` | `reclaim-policy`: `retain`\|`delete` | CR `Subscription`; `<sub>` cũng là tên replication slot |
| `schema-import-source` | `<src>` | `initdb.import` schema-only cho database ứng dụng |
| `replica-source` | `<src>` | `bootstrap.pg_basebackup` + `replica` |
| `replica-enabled` | `true` (mặc định) / `false` = promote | `replica.enabled` |

`Database`, `Publication`, `Subscription` thuộc sở hữu DatabaseCluster và mang nhãn
`everest.percona.com/database-cluster`; gỡ annotation là xoá CR tương ứng. `dataSource`,
`schema-import-source` và `replica-source` loại trừ nhau.

Ví dụ migration một lần từ PostgreSQL ngoài:

```yaml
metadata:
  annotations:
    cnpg.everest.io/source.legacy.host: legacy.example.internal
    cnpg.everest.io/source.legacy.dbname: orders
    cnpg.everest.io/source.legacy.user: migration_user
    cnpg.everest.io/source.legacy.sslmode: verify-full
    cnpg.everest.io/source.legacy.password-secret: migration-source
    cnpg.everest.io/schema-import-source: legacy
    cnpg.everest.io/subscription.dbaas_migration.dbname: app
    cnpg.everest.io/subscription.dbaas_migration.publication: dbaas_migration
    cnpg.everest.io/subscription.dbaas_migration.source: legacy
    cnpg.everest.io/subscription.dbaas_migration.reclaim-policy: delete
```

Biến môi trường của operator:

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `CNPG_WAL_STORAGE_SIZE` | trống (không có volume WAL riêng) | Kích thước volume WAL của cụm mới |
| `CNPG_WAL_STORAGE_CLASS` | trống | StorageClass của volume WAL |
| `CNPG_BACKUP_RETENTION_POLICY` | `7d` | Cửa sổ khôi phục của ObjectStore dùng chung |

## Restore

- `dbClusterBackupName` không PITR: dừng đúng cuối bản backup (`targetImmediate` + `backupID`).
- PITR `date`: phải nằm giữa `completedAt` và `latestRestorableTime` của backup.
- PITR `latest`: phát lại toàn bộ WAL.
- `backupSource.path` dạng `.../<serverName>/base/<backupID>` (chính là `status.destination` của
  `DatabaseClusterBackup`): khôi phục được cả khi cụm nguồn đã bị xoá.

## Webhook từ chối

`spec.paused`, `monitoring.monitoringConfigName` (PMM), `dataImport`, `engineFeatures`,
`engine.crVersion`, `retentionCopies`, proxy khác `pgbouncer`, `proxy.config` ngoài `pool_mode`,
thiết lập pooler khi chưa bật pooler, nâng major, hạ version, đổi `cnpg.everest.io/extension`.

## Chưa làm

Pause/hibernation, switchover và OpsRequest chờ framework OpsRequest; PMM và DataImportJob chưa hỗ
trợ.
