from code import processing
from code import parser


def process_data():
    parser.compile_data()
    processing.process_data()


if __name__ == '__main__':
    process_data()
