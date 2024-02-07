from sys import argv

from utils import database, dataframe
from constants import options, error


def main() -> None:
    if len(argv) < 2:
        exit(error.CLI_ARGVS_EMPTY_ERROR)

    if argv[1] == options.ABA:
        if len(argv) < 3:
            print(error.CLI_ABA_ERROR)
        else:
            filename = argv[2]
            df = dataframe.get_aba_data(filename)
            database.update_aba(df)

    elif argv[1] == options.TRAFFIC:
        if len(argv) < 4:
            print(error.CLI_TRAFFIC_ERROR)
        else:
            firstday, lastday = argv[2], argv[3]
            database.update_traffic(firstday, lastday)

    elif argv[1] == options.TRAFFIC_ABA:
        database.update_traffic_aba()


if __name__ == "__main__":
    main()
