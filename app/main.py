import logging
from pathlib import Path

from make87_messages.core.header_pb2 import Header
import make87
from make87_messages.file.simple_file_pb2 import RelativePathFile
from make87_messages.primitive.bool_pb2 import Bool


def main():
    make87.initialize()
    endpoint = make87.get_provider(
        name="RELATIVE_PATH_FILE", requester_message_type=RelativePathFile, provider_message_type=Bool
    )

    path_prefix = make87.get_config_value("PATH_PREFIX", default="")
    storage_path = make87.get_system_storage_path() / Path(path_prefix)

    def callback(message: RelativePathFile) -> Bool:
        relative_path = Path(message.path)
        if relative_path.is_absolute():
            logging.error("Path is not relative. Returning Success: False.")
            return Bool(
                header=make87.header_from_message(Header, message=message, append_entity_path="success"),
                value=False,
            )

        file_path = storage_path / message.path
        try:
            file_path.write_bytes(message.data)
            logging.info(f"File written to {file_path}.")
        except Exception as e:
            logging.error(f"Error writing file: {e}. Returning Success: False.")
            return Bool(
                header=make87.header_from_message(Header, message=message, append_entity_path="success"),
                value=False,
            )

        return Bool(
            header=make87.header_from_message(Header, message=message, append_entity_path="success"),
            value=True,
        )

    endpoint.provide(callback)
    make87.loop()


if __name__ == "__main__":
    main()
