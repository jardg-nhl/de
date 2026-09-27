# Hướng dẫn trình bày dự án CineInsight

Tài liệu này giải thích **lý do thiết kế và luồng chạy** của project để bạn có thể bảo vệ bài làm. Mã nguồn chính nằm ở `src/pipeline.py`; phần profiling ở `src/ingestion/profiling.py`; DAG ở `dags/cineinsight_pipeline.py`. Các mục “Cách trình bày” có thể dùng làm lời thoại khi thuyết trình.

## 1. Tóm tắt dự án

CineInsight xây dựng nền tảng dữ liệu phân tích từ MovieLens 20M theo kiến trúc Medallion: Landing lưu bản sao nguồn bất biến, Bronze lưu lịch sử có audit, Silver chuẩn hóa nghiệp vụ và kiểm tra chất lượng, Gold phục vụ truy vấn theo mô hình chiều. Pipeline chuyển rating và tag sang cùng một mô hình `InteractionEvent`, dùng batch theo thời gian sự kiện, khóa chống nạp trùng, bảng kiểm soát watermark, quarantine và mô hình SCD cho phim.

**Cách trình bày:**

> “Em tách dữ liệu theo hợp đồng từng tầng. Landing giữ dữ liệu đầu vào nguyên vẹn để replay. Bronze là lịch sử append-only có batch ID, checksum, record hash và số dòng nguồn. Silver là nơi chuẩn hóa và áp dụng quy tắc nghiệp vụ. Gold dùng fact/dimension để truy vấn, trong đó dimension phim có lịch sử thể loại bằng SCD Type 2.”

## 2. Cấu trúc project và nơi đọc code

| Tệp                               | Nội dung cần giải thích                                                           |
| --------------------------------- | --------------------------------------------------------------------------------- |
| `src/pipeline.py`                 | Luồng Landing → Bronze → Silver → Gold, DQ, SCD và báo cáo                        |
| `src/ingestion/profiling.py`      | Đọc CSV theo luồng, đếm record/null, phân bố rating, skew và kiểm tra khóa rating |
| `src/analytics/export_notebook_html.py` | Xuất notebook preview HTML không cần cài Jupyter                            |
| `dags/cineinsight_pipeline.py`    | Lịch chạy, dependency, retry, SLA và timeout của Airflow                          |
| `config/pipeline.json`            | Đường dẫn dữ liệu, database điều khiển, chunk size và số dòng kỳ vọng              |
| `docs/architecture.md`            | Layer contract, mapping CDM, DQ, ERD, SCD và giới hạn của bản local               |
| `notebooks/phase1_profiling.ipynb` | Notebook profiling nguồn                                                         |
| `notebooks/phase5_analytics.ipynb` | Notebook phân tích các mart                                                     |
| `notebooks/phase5_analytics.html`  | Bản HTML xem nhanh các kết quả phân tích                                         |

`README.md` hướng dẫn chạy. `MovieLens/` là dữ liệu đầu vào. `lakehouse/` là thư mục runtime do pipeline tạo, không cần nộp database hay Landing copy.

## 3. Giai đoạn 1 — Ingestion và profiling

### 3.1. Landing

Trong `pipeline.py`, hàm `init()` tạo `lakehouse/landing/` và sao chép sáu CSV vào đó. Nếu file Landing đã tồn tại, hàm so checksum SHA-256 giữa nguồn và bản Landing; file khác nội dung sẽ gây lỗi thay vì ghi đè. Cách này giữ được bản đầu vào bất biến và giúp xác định chính xác nguồn replay.

`sha_file()` tính checksum theo khối 1 MB để không cần nạp toàn bộ file lớn vào RAM. Số dòng kỳ vọng được khai báo ở `EXPECTED_ROWS` để đối soát.

### 3.2. Bronze

Bảng `bronze` lưu `source_file`, `source_system`, `batch_id`, `ingested_at`, `row_number`, `record_hash` và `payload`. `payload` giữ các giá trị nguồn dưới dạng JSON; record lỗi không bị xóa khỏi Bronze. Khóa `(source_file, batch_id, row_number)` làm cho việc chèn lại cùng batch không tạo bản sao.

Rating và tag được đọc theo dòng trong `ingest_event_years()`. Mỗi dòng được đưa vào batch theo năm của `timestamp`; dữ liệu ghi theo chunk để hạn chế RAM và có thể commit từng phần. `control` ghi trạng thái, khoảng sự kiện, số dòng, checksum và watermark. `watermarks` giữ mốc thời gian hoàn thành lớn nhất theo nguồn.

Các dimension nhỏ và `genome_scores` được đọc trong `load_static()`. Với mỗi tệp, số dòng thực đọc được so với `EXPECTED_ROWS`; sai lệch sẽ dừng ingestion.

### 3.3. Profiling thực tế

`src/ingestion/profiling.py` quét từng CSV bằng `csv.DictReader`, không tải toàn bộ 20 triệu dòng vào bộ nhớ. Kết quả đã đo trên dữ liệu được cung cấp:

| Nguồn        | Kết quả đáng chú ý                                                                         |
| ------------ | ------------------------------------------------------------------------------------------ |
| Rating       | 20.000.263 dòng; không ô trống; 138.493 user; 26.744 movie được đánh giá                   |
| Rating key   | File không giảm theo `(userId, movieId)`; không phát hiện cặp trùng nào trong toàn bộ file |
| Rating range | Có đủ 10 mức từ 0,5 đến 5,0; thời gian từ 1995-01-09 đến 2015-03-31                        |
| Skew         | User nhiều rating nhất có 9.254, trung vị 68; movie nhiều rating nhất có 67.310            |
| Tag          | 465.564 dòng, trong đó có 7 tag trống hoặc chỉ có khoảng trắng                             |
| Movie        | 27.278 dòng; 26 title không có năm ở cuối; 246 phim dùng sentinel `(no genres listed)`     |
| Link         | 27.278 dòng; thiếu 252 TMDb ID; IMDb ID có đủ                                              |
| Genome       | 11.709.768 score và 1.128 genome tag                                                       |

Script xác nhận rating file được sắp không giảm theo cặp khóa và không có cặp lặp. Vì thứ tự khóa đã được xác minh trên toàn file, so sánh từng cặp liên tiếp là đủ để kiểm tra trùng trong snapshot này. Nếu thứ tự thay đổi, phải dùng external sort hoặc kho khóa ngoài RAM để kiểm tra chính xác.

Skew khiến partition theo `userId` không phù hợp: vừa tạo nhiều partition nhỏ vừa có user quá lớn. Chiến lược đề xuất là partition rating/tag theo tháng sự kiện; sau đó cluster/bucket theo movie hoặc user nếu engine hỗ trợ. `genome_scores` có thể bucket/hash theo `movieId`; các bảng movie, link, genome tag là dimension nhỏ, không cần partition theo từng ID.

**Cách trình bày:**

> “Em đã kiểm tra dữ liệu thật thay vì chỉ dựa vào mô tả. Rating có đủ 20.000.263 dòng và đủ mười mức điểm. Cặp user/movie không bị lặp trong snapshot sau khi xác minh thứ tự toàn file. Tuy nhiên, mức độ hoạt động lệch đáng kể giữa user và movie, nên em không partition theo user ID.”

## 4. Giai đoạn 2 — Data Quality và quarantine

Hàm `events_to_silver()` kiểm tra từng rating/tag trước khi chuẩn hóa:

- Rating phải nằm trong `[0.5, 5.0]`, bước 0,5.
- ID phải chuyển được sang integer và timestamp phải đọc được.
- Tag trống bị đưa vào `quarantine`; vì đây là free-text không làm mất một rating hợp lệ, nó được phân loại cảnh báo.
- Lỗi timestamp, ID hoặc miền rating là blocking.

Mỗi lỗi được giữ kèm batch, file, số dòng nguồn, error code, lý do và payload. `dq_results` ghi kết quả theo rule/batch. `assert_no_blocking_failures()` dừng pipeline nếu có rule blocking thất bại. `report()` xuất `dq_results.csv` và `quarantine.csv` để người vận hành xem lại.

Các rule referential integrity được tính ở giai đoạn báo cáo: rating phải ghép được với movie; genome score phải ghép được với genome tag; thiếu external ID hoặc title/year là warning. Bản raw vẫn còn trong Landing/Bronze để sửa rồi chạy lại.

**Vì sao không loại record lỗi?** Xóa thầm làm sai đối soát nguồn và không thể truy nguyên. Quarantine giữ dữ liệu gốc và cho phép reprocessing sau khi quy tắc hoặc dữ liệu nguồn được sửa.

## 5. Giai đoạn 3 — CDM và Silver

### 5.1. Mapping sang `InteractionEvent`

| Trường CDM       | MovieLens rating/tag | Giải thích                                                              |
| ---------------- | -------------------- | ----------------------------------------------------------------------- |
| `party_id`       | `userId`             | ID người dùng                                                           |
| `content_id`     | `movieId`            | ID phim                                                                 |
| `event_type`     | Tên bảng             | `RATING` hoặc `TAG`                                                     |
| `event_value`    | `rating × 20`        | Rating 0,5–5,0 đổi thành thang 0–100; TAG để null vì không phải số      |
| `event_text`     | `tag`                | Giữ nội dung tag sau khi trim; rating để null                           |
| `event_time_utc` | `timestamp`          | Chuẩn hóa về ISO UTC; hỗ trợ cả chuỗi thời gian quan sát được lẫn epoch |

Mapping được áp dụng ở `events_to_silver()`. Khi thêm IMDb hoặc log xem phim, ta chỉ cần viết adapter map tên cột, đơn vị điểm và múi giờ về các trường CDM này.

### 5.2. Khóa sự kiện và idempotency

`event_id` là SHA-256 từ nguồn, loại sự kiện, user/movie, thời gian và giá trị nguồn. Nó khác với `record_hash`: `event_id` xác định nghiệp vụ sự kiện để UPSERT; `record_hash` fingerprint nội dung record.

`silver_events` dùng `event_id` làm khóa chính. `silver_processed` ghi các dòng Bronze đã được xử lý bằng `(source_file, batch_id, row_number)`. Khi chạy lại cùng batch, pipeline bỏ qua dòng đã xử lý; record mới có thể được xử lý mà không nhân bản sự kiện. Quy tắc thắng khi cùng event ID xung đột là giữ event-time mới nhất; hash payload được lưu để truy vết.

### 5.3. Watermark và late arrival

Batch rating/tag được cắt theo năm sự kiện. `batch_id` được tạo xác định từ tên nguồn và hai đầu khoảng thời gian nên lần chạy lại nhận ra cùng batch. Watermark chỉ tiến về khoảng kết thúc lớn nhất.

Trong bản local, CSV đơn khối không thể seek theo timestamp, do đó pipeline phải quét tệp để nhận diện dòng mới; chỉ dòng chưa có trong Bronze/Silver được thêm/xử lý. Snapshot này không có luồng event mới thật, nên late-arriving event được mô tả bằng ledger/idempotency và phương án production, chưa được diễn tập bằng file nguồn mới. Thiết kế production nên đặt file Landing bất biến theo ngày/partition, đọc file mới sau watermark và đọc lại một overlap ngắn để bắt late arrivals. Không nên tuyên bố CSV local này đã loại bỏ hoàn toàn việc đọc lại file nguồn.

**Cách trình bày:**

> “Watermark là mốc tiến độ theo event time, còn batch ID xác định khoảng nạp. Để retry an toàn, em dùng event ID cho UPSERT ở Silver và ledger theo số dòng để không xử lý lại record đã hoàn tất. Với CSV snapshot, adapter vẫn quét tệp để tìm record; khi chạy production em sẽ partition file theo ngày để watermark bỏ qua file cũ và cấu hình overlap cho late data.”

## 6. Giai đoạn 4 — Mô hình chiều và SCD

### 6.1. Grain và khóa

- `fact_interaction`: một rating hoặc tag của một user cho một movie tại một thời điểm.
- `fact_genome`: một cặp movie–genome tag và một relevance score.
- `dim_user`: một user MovieLens; `user_sk` là surrogate key.
- `dim_movie`: một phiên bản thông tin movie; `movie_sk` là surrogate key theo từng phiên bản.
- `dim_movie_link`: một movie với IMDb/TMDb ID và URL dựng sẵn.

Surrogate key giúp fact giữ tham chiếu ổn định khi thuộc tính dimension đổi. Business key `movie_id` được giữ lại để tìm các phiên bản của cùng phim.

### 6.2. Lựa chọn SCD

| Thuộc tính                       | SCD    | Lý do                                                                                        |
| -------------------------------- | ------ | -------------------------------------------------------------------------------------------- |
| `genres`                         | Type 2 | Cần biết phim thuộc thể loại nào ở thời điểm lịch sử; thay đổi thể loại có ý nghĩa phân tích |
| `is_deleted`                     | Type 2 | Gỡ phim là thay đổi trạng thái nghiệp vụ; vẫn giữ phim cho fact lịch sử                      |
| Sửa lỗi `title`/`release_year`   | Type 1 | Sửa lỗi hiển thị/metadata; cập nhật qua các phiên bản thay vì coi là thay đổi nghiệp vụ      |
| `previous_title`, `changed_date` | Type 3 | Chỉ cần so sánh nhãn hiện tại với nhãn ngay trước đó                                         |

`movies_to_gold()` đóng phiên bản hiện hành bằng `effective_to`, đặt `is_current=0`, rồi thêm dòng version mới khi thể loại hoặc trạng thái xóa đổi. Mỗi phiên bản có `movie_sk`, `version`, `effective_from`, `effective_to`. Với chỉnh sửa title/year, code cập nhật xuyên các phiên bản và lưu previous title/date ở dòng hiện hành.

Snapshot nguồn không cho biết ngày bắt đầu hiệu lực catalog. Vì vậy baseline được giả định có hiệu lực từ `1900-01-01`, trước các event rating quan sát được; đây là quy ước để point-in-time join có phiên bản cho lịch sử, không phải khẳng định lịch sử catalog thật. CDC giả lập dùng ngày năm 2026.

Point-in-time join dùng điều kiện:

```sql
event_time_utc >= effective_from
AND (effective_to IS NULL OR event_time_utc < effective_to)
```

Nếu chỉ join `is_current=1`, sự kiện cũ có thể bị gán thể loại hiện tại. Movie chưa tới catalog được tạo inferred member `Unknown movie <id>`; khi catalog tới, fact được resolve lại theo phiên bản phù hợp.

**Cách trình bày:**

> “Em chọn Type 2 cho genres vì yêu cầu cần trả lời thể loại tại một thời điểm trong quá khứ. Title và year là metadata sửa lỗi nên dùng Type 1. Previous title phục vụ so sánh ngay trước/sau nên Type 3 là đủ. Fact join với phiên bản movie theo thời gian sự kiện, không join mặc định vào dòng current.”

## 7. Giai đoạn 5 — Analytics và business decisions

`report()` tạo các file CSV trong `lakehouse/reports/` từ Gold:

1. **Xếp hạng phim:** `top_movies.csv` yêu cầu tối thiểu 100 lượt rating. Ngưỡng hạn chế phim chỉ có vài đánh giá cực đoan đứng đầu; số lượt vẫn được xuất để người đọc hiểu độ tin cậy.
2. **Thể loại:** `genre_summary.csv` tính số rating, trung bình và phương sai; phim nhiều thể loại đóng góp vào từng thể loại tương ứng. Bảng sắp theo phương sai giảm dần để nhận diện thể loại bất đồng cao.
3. **Xu hướng:** `rating_by_year.csv` dùng năm cuối title; `rating_by_event_month.csv` dùng tháng người dùng đánh giá. Hai báo cáo mô tả xu hướng, không khẳng định quan hệ nhân quả.
4. **Tag:** chuẩn hóa Unicode NFKC, chữ thường, khoảng trắng và dấu câu; quy một số biến thể phổ biến về `sci-fi`. `popular_tags.csv` đếm mức dùng; `tag_rating_association.csv` tính point-biserial/Pearson giữa sự hiện diện tag trên phim và rating trung bình phim, đồng thời xuất chênh lệch với trung bình toàn cục. Đây là association, không phải tác động nhân quả.
5. **Genome:** `genome_coverage.csv` đo số movie có score trên catalog active; `genome_action_group.csv` mô tả các descriptor có relevance trung bình cao nhất trong nhóm phim Action đang active và số phim hỗ trợ mỗi descriptor. Genome score là dữ liệu suy ra từ mô hình, không phải tag do người dùng nhập.
6. **Hidden gems:** `hidden_gems.csv` chọn rating trung bình ≥4, từ 10 đến 99 lượt đánh giá, phim còn active; kèm URL IMDb zero-pad 7 chữ số và URL TMDb.

Ngưỡng hidden gem được công khai để có thể điều chỉnh. Nếu bỏ ngưỡng rating tối thiểu, một vài lượt đánh giá tình cờ có thể làm phim nổi bật giả tạo.

## 8. Giai đoạn 6 — Orchestration

Trong `dags/cineinsight_pipeline.py`, DAG có ba task theo thứ tự:

1. `landing_verify`: gọi `pipeline.py init`.
2. `bronze_silver_incremental`: gọi `pipeline.py run --as-of {{ ds }}`; task này điều phối các bước xử lý còn lại trong script.
3. `gold_marts_and_dq_report`: gọi `pipeline.py report` để tạo lại mart/report.

`>>` biểu diễn dependency; mỗi task có retry, execution timeout và SLA. Exception từ rule blocking làm task thất bại để Airflow có thể retry/đánh dấu lỗi. `catchup=False` tránh tự chạy hàng loạt ngày cũ khi DAG mới được bật; backfill thủ công truyền ngày cần xử lý làm `--as-of`.

## 9. Vì sao dùng SQLite và giới hạn cần nói rõ

Môi trường local hiện không có Spark, Delta Lake, Iceberg hay Parquet library, nên project dùng SQLite để thể hiện transaction, khóa, UPSERT, audit/control và star schema bằng thư viện Python chuẩn. Đây là reference implementation để đọc/chạy local, không phải lakehouse phân tán. SQLite không thay thế được object storage, catalog, Delta/Iceberg transaction log, distributed MERGE, OPTIMIZE/Z-ORDER hoặc VACUUM.

Nếu được hỏi “đưa lên production thế nào?”, trả lời: giữ nguyên contract/mapping/rule, chuyển file Landing lên S3; Bronze thành append-only Delta/Iceberg; Silver/Gold dùng MERGE theo `event_id`/business key; catalog là Glue/Unity; orchestration là MWAA/Airflow; partition rating theo tháng và theo dõi skew. Không partition theo từng user ID.

## 10. Lệnh chạy và trạng thái materialization

Từ thư mục gốc project:

```powershell
python src/ingestion/profiling.py
python .\src\pipeline.py init
python .\src\pipeline.py run --chunk-size 100000
python .\src\pipeline.py report
python src/analytics/export_notebook_html.py
```

Pipeline đã chạy end-to-end; database điều khiển nằm tại `control/cineinsight.db`, còn Landing và báo cáo nằm trong `lakehouse/`. Regenerate HTML preview sau khi tạo mart để cập nhật các bảng phân tích. Không cần nộp database hay bản sao Landing.

## 11. Câu hỏi giảng viên có thể hỏi

**Tại sao không dùng `(userId, movieId)` làm event ID?**

Trong snapshot này cặp đó duy nhất và đã được kiểm tra. Nhưng CDM hướng tới nhiều nguồn và hành vi; khóa gồm source/type/user/movie/time/value giữ được các sự kiện cùng cặp nếu nguồn sau cho phép đánh giá lại.

**Khác nhau giữa `event_id` và `record_hash`?**

`event_id` là khóa nghiệp vụ để khử trùng và UPSERT. `record_hash` fingerprint nội dung payload phục vụ audit, so sánh phiên bản và lineage.

**Tại sao tag trống là warning mà không dừng toàn pipeline?**

Tag là free-text tùy chọn; một tag trống có thể quarantine mà không làm mất rating hợp lệ. ID/time/rating sai ảnh hưởng khóa hoặc chỉ số chính nên là blocking.

**Tại sao cần SCD Type 2 nếu đã có `movieId`?**

`movieId` nhận diện phim, không nhận diện phiên bản metadata. `movie_sk` gắn fact vào phiên bản phim đúng tại `event_time_utc`.

**Điểm yếu lớn nhất của bản local?**

SQLite và một file CSV nguyên khối không cung cấp scale/seek theo partition như Delta/Iceberg trên object storage. Bản local minh họa hợp đồng và logic; production cần thay adapter lưu trữ và ingest file partitioned.
