import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    print(f"Running dbx-app with catalog={args.catalog} and schema={args.schema}")


if __name__ == "__main__":
    main()