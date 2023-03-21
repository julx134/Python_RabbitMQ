import sys
import copy
import grpc
import GET_COMMANDS_pb2
import GET_COMMANDS_pb2_grpc
import GET_MAP_pb2
import GET_MAP_pb2_grpc
import ast
import os
from hashlib import sha256

import GET_SERIAL_pb2
import GET_SERIAL_pb2_grpc
import POST_PIN_pb2
import POST_PIN_pb2_grpc
import POST_STATUS_pb2
import POST_STATUS_pb2_grpc


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


# function to send pin to server and wait for acknowledgement
def send_pin(pin):
    # establish channel and stub
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = POST_PIN_pb2_grpc.PINStub(channel)
        # send request
        response = stub.SendPin(POST_PIN_pb2.Pin(pin=str(pin)))
        return response.ack


# function to send rover status to server
def send_rover_status(message, status):
    # establish channel and stub
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = POST_STATUS_pb2_grpc.RoverStatusStub(channel)
        # send status
        response = stub.SendStatus(POST_STATUS_pb2.MessageStatus(message=str(message), status=int(status)))
        return response.ack


def disarm_mine(serial_no):
    print(f'Received serial number from server: {serial_no}')

    # note: we increment pin instead of using random to make sure results are reproducible
    # i.e. same time pin is found vs. random time generating random pins
    pin = 0
    success_code = '0' * 4
    mine_key = str(pin) + serial_no
    print('Starting to disarm...')
    while not (hash_ := sha256(f'{mine_key}'.encode()).hexdigest()).startswith(success_code):
        pin += 1
        mine_key = str(pin) + serial_no

    print(f'Found pin: {pin}; Temporary mine key: {hash_}')
    return pin


# run rover commands
def rover_execute_command(path_i, rover_moves, row, col, rover_num):

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

        # rover dies immediately if it steps on a mine and does not immediately dig
        # send notification to server
        if int(rover_map[x][y]) > 0 and move != 'D':
            message = f'Rover {rover_num} stepped on a mine and died'
            ack = send_rover_status(message, 0)
            print(ack)
            return write_path_file(rover_num, path)

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
            case 'D':
                # if rover digs a mine, remove from map
                if int(rover_map[x][y]) > 0:
                    rover_map[x][y] = '0'

                    # get serial mine number from server then disarm mine
                    serial_no = get_serial_no()

                    # disarm mine
                    pin = disarm_mine(serial_no)

                    # then share the pin to server
                    print(send_pin(pin))

        x = rover_pos['x']
        y = rover_pos['y']
        path[x][y] = '*'
        i += 1

    # write path to file and send signal to server that rover has completed moves successfully
    write_path_file(rover_num, path)

    # if successful, send status to server
    message = f'Rover {rover_num} returned home a hero!'
    ack = send_rover_status(message, int(1))
    print(ack)
    return


if __name__ == '__main__':
    rover_num = sys.argv[1]
    row, col, map_list = get_map()
    rover_moves = get_rover_moves(rover_num)
    rover_execute_command(map_list, rover_moves, row, col, rover_num)

