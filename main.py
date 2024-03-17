import json
import traceback
import sys
import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from src.LoadFile import LoadFile
from src.ChartGenerator import ChartGenerator


class CharterWorker:
    def __init__(self) -> None:
        self.queueName: str = "to_charter"
        self.connection: pika.BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", port=5672))
        self.channel: BlockingChannel = self.connection.channel()


    def setupRabbit(self) -> None:
        self.channel.exchange_declare(exchange= "work_exchange", exchange_type= "fanout")
        self.channel.queue_declare(queue= "to_charter")
        self.channel.queue_declare(queue= "to_model")
        self.channel.queue_bind(queue= "to_charter", exchange= "work_exchange")
        self.channel.queue_bind(queue= "to_model", exchange= "work_exchange")

        self.channel.exchange_declare(exchange= "upload_exchange", exchange_type= "direct")
        self.channel.queue_declare(queue= "to_uploader")
        self.channel.queue_bind(queue= "to_uploader", exchange= "upload_exchange")


    def handleData(self, channel: BlockingChannel, method: Basic.Deliver, body: bytes) -> None:
        print(f" [R] {self.queueName}: Starting Worker...")
        try:
            workObject: dict = json.loads(str(body, encoding= "utf-8"))
            loadFile: LoadFile = LoadFile(workObject["url"])
            filePath: str = loadFile.startLoading()
            # filePath: str = "./download/sample-notCheating.csv"
            print("Loaded file:", filePath)
            print("Starting Report Generation...")
            chartGenerator: ChartGenerator = ChartGenerator(filePath)
            chartGenerator.startReportGeneration()
            reportData = chartGenerator.getReportData()
            print("Finished Report Generation.")
            self.channel.basic_publish(
                exchange= "upload_exchange", 
                routing_key="to_uploader", 
                body= json.dumps(reportData)
            )
            print("Sent Data to RabbitMQ.")
        except Exception as e:
            print(traceback.format_exc())
            print(e)
        print(" [R]: Worker Completed.")
        channel.basic_ack(delivery_tag= method.delivery_tag)


    def work(self) -> None:
        self.setupRabbit()
        self.channel.basic_consume(
            queue= "to_charter", 
            on_message_callback= lambda channel, method, _, body: self.handleData(channel, method, body)
        )
        print("Listening on:", self.queueName)
        self.channel.start_consuming()


def main() -> None:
    charterWorker: CharterWorker = CharterWorker()
    charterWorker.work()


if __name__ == "__main__":
    main()