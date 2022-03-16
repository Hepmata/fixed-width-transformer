FROM amazon/aws-lambda-python:3.9

ADD transformer ${LAMBDA_TASK_ROOT}/transformer
ADD slack_notification ${LAMBDA_TASK_ROOT}/slack_notification
ADD slack_statistics ${LAMBDA_TASK_ROOT}/slack_statistics
ADD config ${LAMBDA_TASK_ROOT}/config
COPY requirements.txt .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
