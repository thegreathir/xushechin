import json

import pandas as pd
from telethon import TelegramClient


async def gather_messages(
    client: TelegramClient,
):

    messages_data_frame = pd.DataFrame(
        columns=[
            'channel',
            'id',
            'text',
            'datetime',
            'reply_to',
        ]
    )
    with open('config.json', 'r') as config_fd:
        config = json.load(config_fd)

    for channel in config['channels']:
        async for message in client.iter_messages(
            channel,
            limit=config['message_count']
        ):
            reply_to = await message.get_reply_message()
            new_row = pd.DataFrame({
                'channel': [channel],
                'id': [message.id],
                'text': [message.text],
                'datetime': [message.date],
                'reply_to': [reply_to.id] if reply_to else [None]
            })
            messages_data_frame = pd.concat(
                [
                    messages_data_frame, new_row
                ],
                ignore_index=True,
                axis=0
            )

    return messages_data_frame


def main():
    with open('credentials.json', 'r') as credentials_fd:
        credentials = json.load(credentials_fd)

    with TelegramClient(
        'user',
        credentials['api_id'],
        credentials['api_hash']
    ) as client:
        messages_data_frame = client.loop.run_until_complete(
            gather_messages(client)
        )

    print(messages_data_frame)


if __name__ == '__main__':
    main()
