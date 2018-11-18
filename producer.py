import json
import boto3


class Person:
    def __init__(self, client, data, stream, key):
        '''
        Initialize Person class
        '''
        self.client = client
        self.data = data
        self.stream = stream
        self.key = key

    def put_record(self):
        '''
        Put Data to Kinesis Data Stream
        '''
        response = self.client.put_record(StreamName=self.stream,
                                          Data=self.data,
                                          PartitionKey=self.key)
        return response


if __name__ == '__main__':
    CLIENT = boto3.client('kinesis')
    STREAM = "Hoge"

    while True:
        name = input("Please Enter Name: ")
        person = Person(CLIENT, json.dumps({"name": name}), STREAM, "1")
        res = person.put_record()
        print("Success!! {} {}".format(res['ShardId'], res['SequenceNumber']))
