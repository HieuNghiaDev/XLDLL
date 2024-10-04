from hdfs import InsecureClient
import pandas as pd
import io
from mrjob.job import MRJob

def read_data_from_hdfs(hdfs_path):
    client = InsecureClient('http://localhost:9870', user='dr.who')
    with client.read(hdfs_path) as reader:
        data = pd.read_excel(io.BytesIO(reader.read()))
        return data

def write_data_to_hdfs(hdfs_path, data):
    client = InsecureClient('http://localhost:9870', user='dr.who')
    with client.write(hdfs_path) as writer:
        data.to_excel(writer, index=False)

def download_file_from_hdfs(hdfs_path, local_path):
    client = InsecureClient('http://localhost:9870', user='dr.who')
    client.download(hdfs_path, local_path, overwrite=True)
    print(f"Đã tải tệp từ {hdfs_path} về {local_path}")
    
class SortJob(MRJob):

    def mapper(self, _, line):
        data = line.split(',')
        id_ = data[0]
        name = data[1]
        class_name = data[2]
        poin = float(data[3])
        
        yield poin, line

    def reducer(self, key, values):
        for value in values:
            yield None, value
    
def main():
    input_hdfs_path = "/testbai/test.xlsx"
    output_hdfs_path = "/testbai/kq_sort.xlsx"
    local_download_path = "kq_sort.xlsx"

    try:
        data = read_data_from_hdfs(input_hdfs_path)
        print(f"Đã đọc {len(data)} dòng dữ liệu")
        
        sorted_data = data.sort_values(by='poin')

        write_data_to_hdfs(output_hdfs_path, sorted_data)
        print(f"Đã lưu kết quả sắp xếp lên HDFS: {output_hdfs_path}")
        
        download_file_from_hdfs(output_hdfs_path, local_download_path)
        print(f"Đã tải tệp {local_download_path} về máy để kiểm tra.")

    except Exception as e:
        print(f"Đã xảy ra lỗi: {str(e)}")

if __name__ == "__main__":
    main()