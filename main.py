import os
import boto3
from s3_url import S3Url # 確保你的 s3_url.py 檔案存在且可匯入
from image_classification import classify # 確保你的 image_classification.py 檔案存在且可匯入
import time
from datetime import datetime, timezone
import json
from dotenv import load_dotenv
from botocore.exceptions import ClientError # 引入 ClientError 以處理 AWS 相關錯誤

# --- 載入環境變數 ---
load_dotenv() 

# 從 .env 檔案獲取 AWS 憑證和區域
# 建議使用 .get() 方式獲取，並提供預設值，以防變數未設定
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2") # 預設區域為 ap-northeast-2

# SQS 佇列 URL
queue_url = 'https://sqs.ap-northeast-2.amazonaws.com/881892165012/request-queue-yjche.fifo'

# 結果 S3 桶名稱
results_s3_bucket = 'output-bucket-yjche'

# 從環境變數讀取運行模式，並轉換為布林值
run_continuously = os.getenv('RUN_CONTINUOUSLY', 'False').lower() == 'true'
shutdown_after_completion = os.getenv('SHUTDOWN_AFTER', 'False').lower() == 'true'

# --- 初始化 AWS 客戶端，並明確傳遞憑證 ---
# 這是確保憑證被正確使用的關鍵步驟
sqs_client = boto3.client('sqs', 
                          region_name=aws_region,
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

s3_client = boto3.client('s3', 
                         region_name=aws_region,
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)

ec2_client = boto3.client('ec2', region_name=aws_region, # 只有在需要 EC2 相關操作時才保留
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)


def get_num_messages_available():
    """返回佇列中可見的訊息數量"""
    try:
        response = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['ApproximateNumberOfMessages'])
        messages_available = response['Attributes'].get('ApproximateNumberOfMessages', '0')
        return int(messages_available)
    except ClientError as e:
        print(f"ERROR: AWS SQS Client Error when getting available messages: {e}")
        return 0
    except Exception as e:
        print(f"ERROR: Unexpected error when getting available messages: {e}")
        return 0

def get_num_messages_visible():
    """返回佇列中不可見（正在處理中）的訊息數量"""
    try:
        response = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['ApproximateNumberOfMessagesNotVisible'])
        messages_visible = response['Attributes'].get('ApproximateNumberOfMessagesNotVisible', '0')
        return int(messages_visible)
    except ClientError as e:
        print(f"ERROR: AWS SQS Client Error when getting visible messages: {e}")
        return 0
    except Exception as e:
        print(f"ERROR: Unexpected error when getting visible messages: {e}")
        return 0

def get_latest_message():
    """從 SQS 佇列中獲取一條訊息"""
    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=30, # 訊息在處理期間不可見的秒數 (增加至 30 秒)
            WaitTimeSeconds=5     # 啟用長輪詢，佇列沒有訊息時等待 5 秒
        ) 
        
        # 檢查 response 中是否有 'Messages' 鍵，並且列表不為空
        if 'Messages' not in response or not response['Messages']:
            return None, None # 沒有訊息時返回 None

        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        
        # 解析 SQS 訊息體（它應該是 JSON 字串，包含 S3 事件資訊）
        try:
            s3_event = json.loads(message['Body'])
            bucket_name = s3_event['Records'][0]['s3']['bucket']['name']
            object_key = s3_event['Records'][0]['s3']['object']['key']
            s3_object_path = f"s3://{bucket_name}/{object_key}"
            return s3_object_path, receipt_handle
        except (json.JSONDecodeError, KeyError) as e:
            print(f"ERROR: Message parsing failed. Could not decode JSON or extract S3 info: {e}. Message body: {message['Body']}")
            # 如果解析失敗，不刪除訊息，讓它超時後重新可見，方便調試
            return None, None # 返回 None，讓主循環知道這次沒有有效訊息
    except ClientError as e:
        print(f"ERROR: AWS SQS Client Error when receiving message: {e}")
        return None, None
    except Exception as e:
        print(f"ERROR: Unexpected error when receiving message: {e}")
        return None, None


def delete_message(receipt_handle):
    """從 SQS 佇列中刪除指定句柄的訊息"""
    try:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print(f"SQS: Message with handle {receipt_handle[:10]}... deleted.")
    except ClientError as e:
        print(f"ERROR: AWS SQS Client Error when deleting message {receipt_handle[:10]}...: {e}")
    except Exception as e:
        print(f"ERROR: Unexpected error when deleting message {receipt_handle[:10]}...: {e}")

# 修改後的 put_classification 函數
def put_classification(image_key, classification, s3_location):
    """
    將分類結果寫入 results_s3_bucket 為 JSON 檔案。
    這個 JSON 檔案將包含圖片名、分類結果、S3 位置和時間戳。
    """
    timestamp = datetime.now(timezone.utc).isoformat() # ISO 8601 格式時間戳
    
    result_data = {
        'ImageName': image_key,
        'Classification': classification,
        'OriginalS3Location': s3_location,
        'ClassifiedOn': timestamp
    }
    
    # 創建一個唯一的 S3 鍵名來儲存結果 JSON
    # 例如： test_93_JPEG_cat_2025-06-14T15-00-00Z.json
    s3_result_key = f"{image_key.replace('.', '_')}_{classification.replace(' ', '_')}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}.json"

    try:
        s3_client.put_object(
            Bucket=results_s3_bucket,
            Key=s3_result_key,
            Body=json.dumps(result_data), # 將字典轉為 JSON 字串
            ContentType='application/json'
        )
        print(f"S3 Results: Successfully stored classification for '{image_key}' as '{s3_result_key}' in '{results_s3_bucket}'.")
    except ClientError as e:
        print(f"ERROR: AWS S3 Client Error when storing result for '{image_key}': {e}")
    except Exception as e:
        print(f"ERROR: Unexpected error when storing S3 result for '{image_key}': {e}")

def process_image(s3_object_path):
    """從 S3 獲取圖片，對其分類，然後將結果寫入 S3 和 DynamoDB"""
    print(f"Processing image from S3: {s3_object_path}")
    
    # 解析 S3 路徑 (例如：s3://input-bucket-yjche/test_9.JPEG)
    s = S3Url(s3_object_path)
    
    # 定義本地暫存檔案路徑並確保目錄存在
    temp_dir = '/tmp' # 對於 Linux/EC2。Windows 建議使用 'C:\\Temp' 或其他可寫入路徑
    os.makedirs(temp_dir, exist_ok=True) # 確保暫存目錄存在
    temp_file_path = os.path.join(temp_dir, s.key)

    # 從 S3 下載圖片
    try:
        print(f"S3: Downloading '{s.bucket}/{s.key}' to '{temp_file_path}'...")
        s3_client.download_file(s.bucket, s.key, temp_file_path)
        print("S3: Download complete.")
    except ClientError as e:
        print(f"ERROR: AWS S3 Client Error when downloading '{s.key}': {e}")
        raise # 下載失敗則停止處理此圖片
    except Exception as e:
        print(f"ERROR: Unexpected error when downloading '{s.key}': {e}")
        raise

    # 執行圖片分類
    classification_result = "unknown" # 預設值
    try:
        print(f"Classification: Classifying '{temp_file_path}'...")
        classification_result = classify(temp_file_path) # 假設 classify 函數返回分類結果字串
        print(f"Classification: Result for '{s.key}': '{classification_result}'.")
    except Exception as e:
        print(f"ERROR: Image classification failed for '{s.key}': {e}")
        # 分類失敗也應該記錄，但我們可能仍想把錯誤訊息寫入 DynamoDB 或 S3
        classification_result = f"error: {e}" # 將錯誤作為分類結果

    # 刪除本地暫存檔案
    try:
        os.remove(temp_file_path)
        print(f"Local file: Deleted temporary file '{temp_file_path}'.")
    except OSError as e:
        print(f"WARNING: Could not delete temporary file '{temp_file_path}': {e}")

    # 將結果作為標籤寫入原始 S3 檔案 (位於 input bucket)
    try:
        s3_client.put_object_tagging(
            Bucket=s.bucket, # 這是輸入桶
            Key=s.key,      
            Tagging={
                'TagSet': [
                    {'Key': 'ImageName', 'Value': s.key},
                    {'Key': 'Classification', 'Value': classification_result},
                    {'Key': 'ClassifiedOn', 'Value': str(datetime.now(timezone.utc))}                        
                ]
            }
        )
        print(f"S3: Successfully tagged original object '{s.key}' in input bucket.")
    except ClientError as e:
        print(f"ERROR: AWS S3 Client Error when tagging original object '{s.key}': {e}")
    except Exception as e:
        print(f"ERROR: Unexpected error when tagging original object '{s.key}': {e}")

    # 將分類結果寫入 S3 的 JSON 檔案或帶標籤的物件
    put_classification(s.key, classification_result, s3_object_path)
    print(f"Finished processing image: {s.key}")


def run_worker_loop():
    """主工作者循環，持續從 SQS 接收並處理訊息"""
    print("Worker: Starting main processing loop...")
    while True: # 無限循環，持續從 SQS 接收訊息
        try:
            current_visible_messages = get_num_messages_visible()
            # print(f"Worker: Currently {current_visible_messages} messages visible (in-flight).")
            print(f"Worker: Checking SQS queue '{queue_url}' for new messages...")

            s3_object_path_to_process, receipt_handle = get_latest_message()
            
            if s3_object_path_to_process: # 如果成功獲取到訊息
                print(f"Worker: Received SQS message for {s3_object_path_to_process}.")
                process_image(s3_object_path_to_process) # 處理圖片
                delete_message(receipt_handle) # 處理完畢後刪除訊息
                print(f"Worker: Successfully processed and deleted SQS message for {s3_object_path_to_process}.")
            else: # 如果 get_latest_message 返回 None，表示沒有訊息
                print("Worker: No new messages available in SQS. Waiting...")
                time.sleep(10) # 如果沒有訊息，等待一段時間再嘗試，減少輪詢成本
                
        except ClientError as e: # 捕獲 Boto3 相關的錯誤
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            print(f"CRITICAL ERROR (AWS Client): {error_code} - {error_message}. Retrying in 30 seconds.")
            time.sleep(30) # 如果是 AWS 服務錯誤，等待更長時間避免頻繁重試
        except Exception as e: # 捕獲所有其他未知錯誤
            print(f"CRITICAL ERROR (General): An unexpected error occurred in main loop: {e}. Retrying in 30 seconds.")
            time.sleep(30) # 如果是其他錯誤，也等待更長時間避免無限循環

# --- 腳本主執行區塊 ---
if __name__ == "__main__":
    if run_continuously:
        print("Worker: RUN_CONTINUOUSLY is True. Entering continuous processing mode.")
        run_worker_loop() # 進入無限循環處理訊息
    elif shutdown_after_completion:
        print("Worker: SHUTDOWN_AFTER is True. Processing available messages once, then shutting down.")
        # 如果要精確地只處理「當前」所有訊息，需要一個更複雜的循環
        # 這裡會進入 run_worker_loop 並且無限運行，你需要外部機制來停止它
        # 或修改 run_worker_loop 的條件來退出
        run_worker_loop() # 目前仍然是無限循環，如果設定了 SHUTDOWN_AFTER_COMPLETION，需要人工或外部停止
        
        # --- EC2 實例停止邏輯 (如果適用且 instance_id 已獲取) ---
        # if instance_id != "unknown-instance":
        #     try:
        #         print(f"Job Complete. Shutting down EC2 instance: {instance_id}")
        #         ec2_client.stop_instances(InstanceIds=[instance_id])
        #     except ClientError as e:
        #         print(f"ERROR: Failed to shut down EC2 instance {instance_id}: {e}")
        # else:
        #     print("Job Complete. Instance ID not available for shutdown. Quitting...")
    else:
        print("Worker: Default mode - Running continuously (neither RUN_CONTINUOUSLY nor SHUTDOWN_AFTER is True).")
        run_worker_loop() # 默認行為也是持續運行

    print("Worker: Script finished execution.") # 只有在外部被中斷或有明確退出邏輯時才會印出