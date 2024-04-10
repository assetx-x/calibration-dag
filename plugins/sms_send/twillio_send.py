# import os
#
# from twilio.rest import Client
#
#
# TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
# TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
# TWILIO_PHONE_NUMBER = os.getenv('TWILIO_PHONE_NUMBER')
# RECIPIENT_PHONE_NUMBER = os.getenv('RECIPIENT_PHONE_NUMBER', '+5511984348555')
#
# twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
# print(f'Loaded Twilio client')
#
#
# def send_message(context):
#     status = context['task_instance'].current_state()
#     if status == "success":
#         message = f'Process {context["dag"].dag_id} finished successfully'
#     elif status == "failure":
#         message = f'Process {context["dag"].dag_id} failed'
#     else:
#         message = f'Process {context["dag"].dag_id} status unknown'
#
#     response = twilio_client.messages.create(
#         body=message,
#         from_=TWILIO_PHONE_NUMBER,
#         to=RECIPIENT_PHONE_NUMBER
#     )
#
#     if response.error_code:
#         print(f'[!] Error sending SMS: {response}')
#         return None
#
#     return response.sid
#
# # if __name__ == '__main__':
# #     r = send_sms('Hello from Twilio!')
# #     print(r)
