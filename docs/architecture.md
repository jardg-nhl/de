# CineInsight: Quyết định thiết kế và vận hành

## Kiến trúc và hợp đồng dữ liệu

Snapshot được cung cấp là dữ liệu đầu vào bất biến. Các tệp `lakehouse/landing/*.csv` là bản sao giữ nguyên từng byte; bước khởi tạo sẽ từ chối ghi đè nếu bản sao đã thay đổi. Mỗi batch nguồn được ghi nhận checksum SHA-256. Bronze trong SQLite chỉ ghi nối tiếp: mỗi dòng nguồn trong mỗi batch được lưu thành một bản ghi, giữ nguyên các trường nguồn dưới dạng JSON cùng với hệ thống/tệp nguồn, batch ID, thời điểm nạp, số thứ tự dòng và record hash chuẩn hóa. Bảng control ghi khoảng dữ liệu của lần chạy, checksum, trạng thái, số dòng và watermark. Đây là bản tham chiếu chạy local; khi đưa lên production nên dùng versioning/retention cho object storage và bảng append-only Delta/Iceberg kèm transaction log.

Các sự kiện Silver được chuẩn hóa thành bản ghi `InteractionEvent`. Event ID là SHA-256 của hệ thống nguồn, loại sự kiện, user/movie nguồn, thời điểm sự kiện và giá trị nguồn. Nạp lại batch sẽ UPSERT theo ID này. Nếu có các biểu diễn xung đột nhưng cùng định danh sự kiện, bản ghi có event time lớn nhất, sau đó có record hash lớn nhất theo thứ tự từ điển, sẽ được chọn để kết quả ổn định. Profiling kiểm tra toàn bộ tệp rating có thứ tự không giảm theo `(userId,movieId)` và không phát hiện cặp trùng trong snapshot này; tag có thể lặp tổ hợp user/movie/tag. Timestamp vẫn nằm trong khóa sự kiện CDM để không làm mất sự kiện từ các nguồn tương lai cho phép chấm lại. Các event ID trùng được gộp; dòng nguồn vẫn có thể truy vết trong Bronze.

Các cửa sổ event time được chia theo năm dương lịch, bao phủ các timestamp có trong từng nguồn. Lịch sử rating bắt đầu năm 1995; tag bắt đầu năm 2005. Batch ID gồm nguồn và khoảng thời gian chính xác nên có thể tiếp tục sau khi bị gián đoạn. Tham số `--as-of` giới hạn phạm vi xử lý để backfill. Watermark lưu mốc kết thúc lớn nhất đã hoàn tất. ID dòng Bronze và sổ theo dõi xử lý Silver cho phép một dòng đến muộn được thêm vào batch của năm có event time tương ứng rồi hợp nhất đúng một lần. Vì CSV đơn khối không thể seek theo event time, adapter local phải quét tệp để phát hiện dòng mới; chỉ những dòng chưa xử lý mới được ghi/xử lý. Khi triển khai production, nên landing các tệp bất biến đã phân vùng theo ngày và dùng khoảng chồng lấn có thể cấu hình (ví dụ 7 ngày), giữ cơ chế idempotency theo event ID và chỉ tăng watermark sau khi đối soát cùng Silver commit thành công.

CDC cho catalog phim được giả lập theo cách xác định và gắn nhãn rõ là `synthetic-cdc`: cứ mỗi 5.000 dòng catalog sẽ tạo một INSERT với ID mới, một UPDATE thể loại và một soft DELETE. Đây là luồng minh họa, không khẳng định đó là các thay đổi thực tế của catalog. Có thể replay an toàn nhờ khóa batch và số dòng.

## Ánh xạ CDM

| Trường CDM | Ánh xạ nguồn | Quy tắc |
|---|---|---|
| `party_id` | `rating.userId`, `tag.userId` | Định danh user dạng số nguyên từ nguồn |
| `content_id` | `rating.movieId`, `tag.movieId` | Movie ID dạng số nguyên của MovieLens |
| `event_type` | Tên bảng nguồn | `RATING` hoặc `TAG` |
| `event_value` | rating hoặc tag | Rating được đổi sang thang 0–100 (`rating * 20`); tag là thuộc tính văn bản của sự kiện nên giá trị số là null |
| `event_time_utc` | `timestamp` | Tệp thực tế dùng `YYYY-MM-DD HH:mm:ss`; parse thành UTC. Adapter cũng chấp nhận Unix epoch |
| `content title/year/genres` | `movie.title`, `movie.genres` | Trim title, dùng regex lấy năm ở cuối dạng `(YYYY)`, tách genres thành JSON array; sentinel không có thể loại được đổi thành array rỗng |

Nguồn mới được ánh xạ ở ranh giới adapter; schema `InteractionEvent` vẫn giữ ổn định. Tag văn bản tự do được giữ nguyên cách viết trong Bronze và được trim để kiểm tra; nếu cần gộp từ đồng nghĩa về mặt ngữ nghĩa thì nên chuẩn hóa trong một text dimension riêng. Không tự động gộp `scifi` với `sci-fi` nếu chưa có synonym map được quản trị.

## Quy tắc chất lượng và quarantine

**Blocking:** không đọc được nguồn hoặc checksum không khớp; timestamp sự kiện không hợp lệ; rating nằm ngoài [0.5, 5.0] hoặc không theo bước 0.5; ID bắt buộc không hợp lệ. **Warning:** thiếu năm ở cuối title; sentinel không có thể loại; tham chiếu mồ côi tới catalog/link/genome; ID tùy chọn rỗng; tần suất user/movie bất thường; event identity lặp. Tag rỗng và giá trị bắt buộc sai định dạng được đưa vào `quarantine` cùng batch, dòng nguồn, mã lỗi, lý do và payload; dòng gốc vẫn được giữ trong Bronze. Cách khắc phục là gửi dòng nguồn đã sửa trong batch sau rồi chạy lại thao tác merge Silver. Kết quả DQ được lưu theo batch/rule. Đối soát so checksum Landing đầy đủ và số dòng parse được với số dòng kỳ vọng trước khi đánh dấu batch thành công.

Profiling nên bao gồm số dòng/null chính xác, suy luận kiểu dữ liệu, trùng PK/business key, miền rating, khoảng timestamp, ngoại lệ genre/năm của movie, anti-join khóa ngoại, số dòng theo từng khóa và kích thước partition. Tập user trong rating rộng hơn nhiều so với tag. Với rating, partition theo ngày sự kiện (theo tháng khi bảng lớn), sau đó cluster/bucket theo content/user nếu engine hỗ trợ; không partition theo userId vì sẽ tạo nhiều partition nhỏ và gây skew. Tag cũng nên partition theo tháng sự kiện; movie/link/genome_tags là dimension nhỏ; genome_scores có thể chia range/hash hoặc bucket theo movieId, có thể kết hợp tagId. Tránh tạo một partition cho mỗi category hay ID có cardinality cao. Theo dõi tỷ lệ p99/max so với median số dòng mỗi partition và các khóa join bị skew.

### Kết quả profiling trên các tệp được cung cấp

| Nguồn | Số dòng | Phát hiện |
|---|---:|---|
| rating | 20.000.263 | 0 ô rỗng; 138.493 user; 26.744 movie được đánh giá; giá trị theo bước nửa sao hợp lệ; xác nhận không trùng cặp `(userId,movieId)` trên toàn tệp đã sắp thứ tự; sự kiện từ 1995-01-09 đến 2015-03-31 |
| tag | 465.564 | 7 tag rỗng/chỉ có khoảng trắng; quarantine theo mức warning và vẫn giữ dòng nguồn |
| movie | 27.278 | 26 title không có năm ở cuối; 246 dòng dùng `(no genres listed)` |
| link | 27.278 | Thiếu 252 TMDb ID; IMDb ID có đủ |
| genome_scores | 11.709.768 | Bảng điểm suy diễn dày; kiểm tra miền giá trị và khóa ngoại khi nạp |
| genome_tags | 1.128 | Bảng tra cứu nhỏ |

Skew của rating đáng kể: user hoạt động nhiều nhất có 9.254 rating so với trung vị 68 rating/user; movie có nhiều rating nhất đạt 67.310. Điều này ủng hộ partition theo ngày và giám sát skew rõ ràng, có thể dùng salting cho join lệch nghiêm trọng, thay vì partition theo user hoặc movie. Phân bố rating đạt đỉnh ở 4.0 (5.561.926 lượt) và có đủ 10 giá trị nửa sao như kỳ vọng.

## Mô hình chiều và SCD

```mermaid
erDiagram
  DIM_USER ||--o{ FACT_INTERACTION : party
  DIM_MOVIE ||--o{ FACT_INTERACTION : content_version
  DIM_MOVIE ||--o{ FACT_GENOME : describes
  DIM_GENOME_TAG ||--o{ FACT_GENOME : labels
  DIM_MOVIE ||--|| DIM_MOVIE_LINK : external_ids
  DIM_MOVIE { int movie_sk PK; int movie_id; string title; int release_year; string genres_json; datetime effective_from; datetime effective_to; bool is_current; int version }
  DIM_USER { int user_sk PK; int user_id; datetime first_seen }
  FACT_INTERACTION { string event_id PK; int user_sk FK; int movie_sk FK; string event_type; float event_value; datetime event_time_utc }
  FACT_GENOME { int movie_id FK; int tag_id FK; float relevance }
```

Grain của `fact_interaction`: một sự kiện rating hoặc tag từ một nguồn tại một thời điểm. Grain của `fact_genome`: một điểm relevance suy diễn cho mỗi cặp movie–genome tag. Surrogate key của movie và user tách mô hình phân tích khỏi thay đổi khóa nguồn và hỗ trợ join lịch sử. ID nguồn bên ngoài là thuộc tính degenerate trong link dimension quan hệ 1–1; ID ngoài bị thiếu được giữ null.

Movie `genres` dùng SCD Type 2 vì yêu cầu cần biết thể loại tại một thời điểm trong quá khứ và thay đổi thể loại ảnh hưởng phân tích lịch sử. Soft delete cũng được version hóa để sự kiện lịch sử vẫn trỏ tới phiên bản movie có hiệu lực tại thời điểm đó. Title và year được đính chính bằng SCD Type 1: giá trị sửa được cập nhật qua mọi phiên bản vì đây là sửa lỗi hiển thị/metadata, không phải thay đổi trạng thái nghiệp vụ lịch sử. `previous_title` và `changed_date` cung cấp góc nhìn Type 3 về lần sửa nhãn liền trước. Dataset không có user dimension từ nguồn, nên chỉ tạo inferred member cho user xuất hiện trong event; nếu sau này có thuộc tính phân khúc user cần báo cáo lịch sử thì nên dùng Type 2, còn sửa lỗi chính tả/định dạng thì dùng Type 1.

Điều kiện point-in-time join: `event_time_utc >= effective_from AND (effective_to IS NULL OR event_time_utc < effective_to)`. Join thông thường vào dòng hiện tại chỉ trả lời movie đang như thế nào và có thể gán sai thể loại cho sự kiện lịch sử. Dòng movie ban đầu được xem là inferred member nếu sự kiện đến trước catalog; khi catalog đến sau, inferred member được bổ sung và fact được ánh xạ lại. Dataset này có đủ movie ID cho các sự kiện bình thường, nhưng quy tắc này hỗ trợ các nguồn tương lai.

## Lựa chọn phân tích

Xếp hạng phim yêu cầu tối thiểu 100 rating để giảm biến động do cỡ mẫu nhỏ; đồng thời xuất số lượt rating và phương sai. Nếu không đặt ngưỡng, phim chỉ có một lượt rating 5 sao có thể đứng đầu. Trung bình và phương sai tổng thể theo thể loại được tính theo từng lượt rating; phim có nhiều thể loại đóng góp vào từng thể loại tương ứng. Báo cáo xu hướng theo năm phát hành và theo tháng rating; các xu hướng này không hàm ý quan hệ nhân quả. Tag được chuẩn hóa bằng Unicode NFKC, trim, casefold, gộp khoảng trắng và chính sách dấu câu trong trường dẫn xuất có kiểm soát. `tag_rating_association.csv` tính tương quan point-biserial (Pearson) giữa sự hiện diện của tag đã chuẩn hóa và rating trung bình theo movie trên các movie có rating, đồng thời xuất trung bình nhóm có tag và trung bình chung; kết quả mô tả sự liên hệ, không khẳng định quan hệ nhân quả. `genome_action_group.csv` tóm tắt các descriptor có relevance trung bình cao trong nhóm movie Action đang active, cùng độ phủ movie của từng descriptor. Genome coverage là tỷ lệ movie trong catalog có ít nhất một score; genome relevance là dữ liệu do thuật toán suy ra, không phải tag người dùng nhập. Hidden gem được xác định bằng chất lượng >=4.0, từ 10 đến 99 rating và movie đang active, không bị xóa mềm; các ngưỡng được công khai để có thể điều chỉnh. IMDb ID được đệm số 0 về 7 chữ số khi dựng URL; TMDb URL dùng ID số nguyên.

## Vận hành

Các bước trong Airflow DAG: xác minh Landing → nạp Bronze/đối soát → merge Silver và blocking DQ → merge catalog/SCD movie → nạp dimensions/facts → tạo marts. Retry an toàn vì đầu ra các bước có khóa ổn định và idempotent. Backfill nhận ngày logical date và gọi cùng quy trình với khoảng thời gian giới hạn tương ứng. Khi triển khai Delta, dùng transaction MERGE, partition pruning, OPTIMIZE/Z-order trên content/date thường được lọc và VACUUM theo chính sách retention. SQLite không có transaction history của Delta, khả năng xử lý phân tán, schema evolution native hay time travel cấp bảng; đây là các phần adapter production cần bổ sung.
