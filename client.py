import json
import sys
import copy
import grpc
import GET_COMMANDS_pb2
import GET_COMMANDS_pb2_grpc
import GET_MAP_pb2
import GET_MAP_pb2_grpc
import ast
import os
import pika
import GET_SERIAL_pb2
import GET_SERIAL_pb2_grpc


# function that sends a request to grpc server to get map
def get_map():
    # establish channel and stub
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = GET_MAP_pb2_grpc.MapStub(channel)
        # send request
        response = stub.GetMap(GET_MAP_pb2.MapRequest(name="map.txt"))

        # return response -- ast.literal_eval is to clean-up string formatting to be converted back to list
        return response.row, response.col, list(ast.literal_eval(response.map))


# function to get stream of commands for rover
def get_rover_moves(rover_num):
    # establish channel and stub
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = GET_COMMANDS_pb2_grpc.RoverCommandsStub(channel)
        # send request
        response = stub.GetRoverMoves(GET_COMMANDS_pb2.RoverNum(rover_name=str(rover_num)))
        return response.commands


# function to write path file of rover
def write_path_file(i, path):
    absolute_path = os.path.dirname(__file__)
    rover_folder_path = 'rover_paths'
    rover_dir = os.path.join(absolute_path, rover_folder_path)

    with open (os.path.join(rover_dir, "path_{}.txt".format(i)), 'w+') as txt_file:
        for line in path:
            txt_file.write(" ".join(line)+'\n')


# function to send request to server to get serial num
def get_serial_no():
    # establish channel and stub
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = GET_SERIAL_pb2_grpc.SerialStub(channel)
        # send request
        response = stub.GetSerial(GET_SERIAL_pb2.PlaceHolder(place_holder=None))
        return response.serial_no

def start_rabbitmq_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.queue_declare(queue='demine-queue')

    return channel, connection


# run rover commands
def rover_execute_command(path_i, rover_moves, row, col, rover_num):

    channel, connection = start_rabbitmq_channel()

    # copy map to list -- this is so we don't have to write to map.txt directly
    rover_map = copy.deepcopy(path_i)

    # initialize path map for rover
    path = [['0' for x in range(col)] for j in range(row)]

    # dictionary to track rover position
    rover_pos = {'x': 0, 'y': 0, 'dir': 'S'}

    i = 0
    outer_x_bounds = row - 1
    outer_y_bounds = col - 1
    x = rover_pos['x']
    y = rover_pos['y']
    path[x][y] = '*'

    # for loop and match-case statements that handle rover movement
    for move in rover_moves:

        # if rover finds a mine, request serial number and send to demine queue
        if int(rover_map[x][y]) > 0:
            # get mine location and update map
            rover_map[x][y] = '0'

            # get serial mine number from server
            print("Requesting serial_no from server...")
            serial_no = get_serial_no()
            print(f"Received serial_no:{serial_no}")

            # publish mine data to queue
            mine_data = {'x': x, 'y': y, 'serial_no': serial_no}
            print(f"Sending data over to queue", mine_data)
            # add serial number and location to demine queue
            channel.basic_publish(exchange='', routing_key='demine-queue', body=json.dumps(mine_data))
        match move:
            case 'M':  # move forward
                match rover_pos['dir']:
                    case 'S':
                        if rover_pos['x'] + 1 <= outer_x_bounds:
                            rover_pos['x'] += 1
                    case 'N':
                        if rover_pos['x'] - 1 >= 0:
                            rover_pos['x'] -= 1
                    case 'W':
                        if rover_pos['y'] - 1 >= 0:
                            rover_pos['y'] -= 1
                    case 'E':
                        if rover_pos['y'] + 1 <= outer_y_bounds:
                            rover_pos['y'] += 1
            case 'L':  # turn left
                match rover_pos['dir']:
                    case 'S':
                        rover_pos['dir'] = 'E'
                    case 'N':
                        rover_pos['dir'] = 'W'
                    case 'W':
                        rover_pos['dir'] = 'S'
                    case 'E':
                        rover_pos['dir'] = 'N'
            case 'R':  # turn right
                match rover_pos['dir']:
                    case 'S':
                        rover_pos['dir'] = 'W'
                    case 'N':
                        rover_pos['dir'] = 'E'
                    case 'W':
                        rover_pos['dir'] = 'N'
                    case 'E':
                        rover_pos['dir'] = 'S'

        x = rover_pos['x']
        y = rover_pos['y']
        path[x][y] = '*'
        i += 1

    # write path to file and send signal to server that rover has completed moves successfully
    write_path_file(rover_num, path)

    # if successful, send status to server
    message = f'Rover {rover_num} finished exploring the map'
    print(message)
    connection.close()
    return


if __name__ == '__main__':
    rover_num = sys.argv[1]
    row, col, map_list = get_map()
    rover_moves = get_rover_moves(rover_num)
    rover_execute_command(map_list, rover_moves, row, col, rover_num)

