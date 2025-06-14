import torch
import torchvision.transforms as transforms
import torchvision.models as models
from PIL import Image
import json
import sys
import numpy as np
import os # <-- 新增：導入 os 模組

def classify(path):
    img = Image.open(path)
    model = models.resnet18(pretrained=True)

    model.eval()
    img_tensor = transforms.ToTensor()(img).unsqueeze_(0)
    outputs = model(img_tensor)
    _, predicted = torch.max(outputs.data, 1)

    # --- 修改開始 ---
    # 獲取當前腳本所在的目錄
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    # 建構 imagenet-labels.json 的完整路徑
    # 假設 imagenet-labels.json 和您的 classify.py 檔案在同一個目錄下
    labels_file_path = os.path.join(current_script_dir, 'imagenet-labels.json')

    try:
        with open(labels_file_path, 'r', encoding='utf-8') as f: # 建議加上 encoding='utf-8'
            labels = json.load(f)
        result = labels[np.array(predicted)[0]]
        return(f"{result}")
    except FileNotFoundError:
        # 如果找不到檔案，打印更詳細的錯誤訊息
        print(f"錯誤：找不到標籤檔案。請確保 '{labels_file_path}' 存在。")
        return "error: Labels file not found"
    except Exception as e:
        print(f"處理分類結果時發生錯誤：{e}")
        return "error: Classification processing failed"
    # --- 修改結束 ---
