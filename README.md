# Cloud Functions in Python with pub/sub emulator local

<br/>

---

## Requeriments

#### GCLOUD CLIENT [YOU CAN INSTALL HERE](https://cloud.google.com/sdk/docs/install) <br>
#### PUBSUB COMPONENT INSTALLED 
```
gcloud components install pubsub-emulator
```
#### PYTHON SDK [YOU CAN INSTALL HERE](https://www.python.org/downloads/)

## Getting started

```
git clone https://github.com/jorbinogales/pubsub_python_cloud_function_test_local
cd pubsub_python_cloud_function_test_local
pip install -r requeriments.txt
```

## To run the cloud functions frameworks

```
functions-framework --source main.py --target=my_function --signature-type=http --debug
```

### You get something like this

```
* Serving Flask app 'my_function'
* Debug mode: on
  2023-12-16 11:33:24,389 - INFO - WARNING: This is a development server. Do not use it in a production deployment. Use a production WSGI server instead.
* Running on all addresses (0.0.0.0)
* Running on http://127.0.0.1:8080
* Running on http://192.168.1.11:8080
  2023-12-16 11:33:24,404 - INFO - Press CTRL+C to quit
  2023-12-16 11:33:24,420 - INFO -  * Restarting with watchdog (windowsapi)
  2023-12-16 11:33:25,414 - WARNING -  * Debugger is active!
  2023-12-16 11:33:25,414 - INFO -  * Debugger PIN: 182-292-905
```

## Active the trigger

Now yo need to active the trigger so, yo need send the request to the <strong> http://localhost:127.0.0.1:8080</strong> 

```
GET {{host}} HTTP/1.1
Content-Type: {{contentType}}
```

### Response
```
Procesado
```