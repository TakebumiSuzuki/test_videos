import os
from google.cloud import storage
from google.cloud.speech_v2 import SpeechClient
from google.cloud.speech_v2.types import cloud_speech

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# def transcribe_long_audio(project_id, location, gcs_uri):
#     """Transcribe long audio file from GCS using asynchronous speech recognition"""
#     client = speech_v2.SpeechClient()

#     # Recognizerの設定
#     recognizer = speech_v2.Recognizer(
#         model="chirp_2",  # 最新のモデルを使用
#         language_codes=["auto"],  # 自動言語検出
#     )

#     # Recognizerの作成
#     parent = f"projects/{project_id}/locations/{location}"
#     create_recognizer_request = speech_v2.CreateRecognizerRequest(
#         parent=parent,
#         recognizer=recognizer,
#         recognizer_id="",  # 空文字列で一意のIDが自動生成されます
#     )

#     operation = client.create_recognizer(request=create_recognizer_request)
#     created_recognizer = operation.result()

#     # 非同期認識リクエストを設定
#     config = speech_v2.RecognitionConfig(
#         auto_decoding_config=speech_v2.AutoDetectDecodingConfig(),
#     )

#     file_metadata = speech_v2.BatchRecognizeFileMetadata(uri=gcs_uri)

#     request = speech_v2.BatchRecognizeRequest(
#         recognizer=created_recognizer.name,  # ここが正しい使用法です
#         config=config,
#         files=[file_metadata],
#     )

#     # 非同期認識を開始
#     operation = client.batch_recognize(request=request)

#     print("Waiting for operation to complete...")
#     response = operation.result(timeout=3600)  # タイムアウトを1時間に設定

#     # 結果を処理
#     for result in response.results[gcs_uri].transcript.results:
#         print(f"Transcript: {result.alternatives[0].transcript}")
#         print(f"Detected language: {result.language_code}")

#     # Recognizerを削除（オプション）
#     client.delete_recognizer(name=created_recognizer.name)





def transcribe_batch_gcs_input_inline_output_v2(audio_uri: str, project_id: str, location: str) -> cloud_speech.BatchRecognizeResults:
    # Instantiates a client
    client = SpeechClient()

    config = cloud_speech.RecognitionConfig(
        auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
        language_codes=["auto"],
        model="chirp_2",
    )

    file_metadata = cloud_speech.BatchRecognizeFileMetadata(uri=audio_uri)

    request = cloud_speech.BatchRecognizeRequest(
        recognizer=f"projects/{project_id}/locations/{location}/recognizers/_",
        config=config,
        files=[file_metadata],
        recognition_output_config=cloud_speech.RecognitionOutputConfig(
            inline_response_config=cloud_speech.InlineOutputConfig(),
        ),
    )

    # Transcribes the audio into text
    operation = client.batch_recognize(request=request)

    print("Waiting for operation to complete...")
    response = operation.result(timeout=600)

    for result in response.results[audio_uri].transcript.results:
        print(f"Transcript: {result.alternatives[0].transcript}")

    return response.results[audio_uri].transcript







# 使用例
if __name__ == "__main__":
    project_id = "arvato-developments"
    location = "global"
    bucket_name = "your-bucket-name"
    source_file_name = "daidan_test.mp3"
    destination_blob_name = "audio/long_audio_file.wav"

    # ローカルファイルをGCSにアップロード
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    # GCS URI
    audio_uri = f"gs://{bucket_name}/{destination_blob_name}"

    # 非同期処理を開始
    transcribe_batch_gcs_input_inline_output_v2(audio_uri, project_id, location)