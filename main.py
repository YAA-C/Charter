import json
import traceback
import sys
import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from src.LoadFile import LoadFile
from src.ChartGenerator import ChartGenerator
from src.utils.Logger import log, logp, compileLogs


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


    def rejectMessage(self, channel: BlockingChannel, method: Basic.Deliver) -> None:
        if(method.redelivered):
            log("Discarding message from queue...")
            channel.basic_reject(delivery_tag= method.delivery_tag, requeue= False)
        else:
            log("Requeuing message...")
            channel.basic_reject(delivery_tag= method.delivery_tag, requeue= True)


    def handleData(self, channel: BlockingChannel, method: Basic.Deliver, body: bytes) -> None:
        log("Job Received.")
        try:
            workObject: dict = json.loads(str(body, encoding= "utf-8"))
            loadFile: LoadFile = LoadFile(workObject["url"])
            matchId: str = str(workObject["match_id"])
            filePath: str = loadFile.startLoading()
            # matchId: str = "TEST"
            # filePath: str = "./download/sample-notCheating.csv"
            log("Loaded file:", filePath)
            charterData: list = []
            log("Starting Report Generation...")
            chartGenerator: ChartGenerator = ChartGenerator(filePath, matchId)
            chartGenerator.startReportGeneration()
            charterData.append(chartGenerator.getReportData())
            log("Finished Report Generation.")
            self.channel.basic_publish(
                exchange= "upload_exchange", 
                routing_key="to_uploader", 
                body= json.dumps(charterData)
            )
            log("Sent Data to RabbitMQ.")
        except Exception:
            log(traceback.format_exc())
            self.rejectMessage(channel, method)
        else:
            channel.basic_ack(delivery_tag= method.delivery_tag)
        finally:
            log("Job Completed.")


    def work(self) -> None:
        self.setupRabbit()
        self.channel.basic_consume(
            queue= "to_charter", 
            on_message_callback= lambda channel, method, _, body: self.handleData(channel, method, body)
        )
        log("Listening on:", self.queueName)
        self.channel.start_consuming()


def main() -> None:
    try:
        log("Starting Worker...")
        charterWorker: CharterWorker = CharterWorker()
    except Exception as e:
        log("Could not connect to RabbitMQ...")
        log(traceback.format_exc())
    else:
        charterWorker.work()
    finally:
        log("Exitting Worker...")
        compileLogs(["CW"])


if __name__ == "__main__":
    main()