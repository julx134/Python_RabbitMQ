import grpc
from concurrent import futures
import GET_COMMANDS_pb2
import GET_COMMANDS_pb2_grpc
import GET_MAP_pb2_grpc
import GET_MAP_pb2
from urllib.request import urlopen
import json
import uuid

import GET_SERIAL_pb2
import GET_SERIAL_pb2_grpc
import POST_PIN_pb2
import POST_PIN_pb2_grpc
import POST_STATUS_pb2
import POST_STATUS_pb2_grpc

# global url variable for API endpoint
urlBase = "https://coe892.reev.dev/lab1/rover/"


# Gets the map and sends it to the client
class Map(GET_MAP_pb2_grpc.MapServicer):
    print('Received map request')

    # override GetMap proto method
    def GetMap(self, request, context):
        # open file
        with open(str(request.name)) as file:
            # get dimensions
            dim = file.readline().split()
            row = int(dim[0])
            col = int(dim[1])

            # format map as array of string
            map_file = list()
            for line in file:
                map_file.append(line.split())

        # send map
        print(f'Sending {request.name}')
        return GET_MAP_pb2.MapResponse(col=col, row=row, map=str(map_file))


# Gets the map and sends it to the client
class RoverCommands(GET_COMMANDS_pb2_grpc.RoverCommandsServicer):
    print('Received get rover command request')

    # override GetRoverMoves proto method
    def GetRoverMoves(self, request, context):
        endpoint = urlBase + str(request.rover_name)

        # store the response of URL
        response = urlopen(endpoint)

        # convert and store response to JSON format
        data_json = json.loads(response.read())

        # assign values to rovers dict
        rover_moves = data_json['data']['moves']

        # send map
        print(f'Sending rover {request.rover_name} commands')
        return GET_COMMANDS_pb2.Commands(commands=str(rover_moves))


# Send randomly generated serial upon rover request
class SerialMine(GET_SERIAL_pb2_grpc.SerialServicer):
    print('Received send serial number request')

    # override GetSerial proto method
    def GetSerial(self, request, context):
        # generate random id based on rover name and num move
        serial_no = uuid.uuid4().hex
        print(f'Generated serial {serial_no}')
        return GET_SERIAL_pb2.SerialNumber(serial_no=str(serial_no))


class ReceivePin(POST_PIN_pb2_grpc.PINServicer):
    # override SendPin proto method
    def SendPin(self, request, context):
        # generate random id based on rover name and num move
        print(f'Received pin: {request.pin}')
        ack = f'Server acknowledges pin: {request.pin}'
        return POST_PIN_pb2.Acknowledgement(ack=ack)


class ReceiveRoverStatus(POST_STATUS_pb2_grpc.RoverStatusServicer):
    # override SendStatus proto method
    def SendStatus(self, request, context):
        # process status of rover and send acknowledgement messages accordingly
        status = request.status
        print(request.message)

        if status:
            ack = f'Congratulations on making it home safe!'
        else:
            ack = f'Try again next time!'

        return POST_STATUS_pb2.AcknowledgementStatus(ack=ack)
def start_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    server.add_insecure_port('[::]:50051')
    GET_MAP_pb2_grpc.add_MapServicer_to_server(Map(), server)
    GET_COMMANDS_pb2_grpc.add_RoverCommandsServicer_to_server(RoverCommands(), server)
    GET_SERIAL_pb2_grpc.add_SerialServicer_to_server(SerialMine(), server)
    POST_PIN_pb2_grpc.add_PINServicer_to_server(ReceivePin(), server)
    POST_STATUS_pb2_grpc.add_RoverStatusServicer_to_server(ReceiveRoverStatus(), server)
    server.start()
    print('Started Server...')
    server.wait_for_termination()


start_server()