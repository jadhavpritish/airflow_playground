import logging
import os
import pathlib
from typing import Any
from urllib.parse import urljoin

import pendulum
import requests
from airflow.decorators import dag, task
from requests.exceptions import ConnectionError, MissingSchema

logger = logging.getLogger(__name__)

# https://github.com/apache/airflow/discussions/24463
# https://stackoverflow.com/questions/75980623/why-is-my-airflow-hanging-up-if-i-send-a-http-request-inside-a-task
os.environ["NO_PROXY"] = "*"


HOST_URL = "https://ll.thespacedevs.com"


def get_future_launches():

    logger.info("before requests call is made")
    resp = requests.request(
        "GET", url=urljoin(HOST_URL, "2.0.0/launch/upcoming/"), params={"limit": 10}
    )

    logger.info("resp collected")
    resp.raise_for_status()
    logger.info("raise for status done")

    return resp.json()


@dag(
    schedule=None,
    start_date=pendulum.today("UTC").add(days=-14),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["rocket_images"],
)
def rocket_dag():

    @task()
    def hello_world():
        for i in range(10):
            logger.info(f"Hello World {i}")

        return "Hello World completed"

    @task()
    def extract():
        logger.info("getting response from spacedev")
        future_launches = get_future_launches()
        logger.info("received_response")
        return future_launches

    @task()
    def transform(rocket_launches: dict[str, Any]):
        pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

        image_urls = [launch["image"] for launch in rocket_launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except ConnectionError:
                print(f"Could not connect to {image_url}.")

    res1 = hello_world()
    res2 = extract()
    res3 = transform(res2)

    res1 >> res2 >> res3


rocket_dag()


# notify = BashOperator(
#    task_id="notify",
#    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
#    dag=rocket_dag,
# )
