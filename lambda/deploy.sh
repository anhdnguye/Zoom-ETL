# lambda/deploy.sh
cd lambda/zoom_webhook
zip -r zoom_webhook.zip .

aws lambda update-function-code \
  --function-name ZoomRecordingReceiver \
  --zip-file fileb://zoom_webhook.zip
