import json
from datetime import datetime

import pandas as pd
import pytz
from telethon import TelegramClient
from tqdm import tqdm


async def gather_messages(
    client: TelegramClient,
    config: dict
):

    messages_data_frame = pd.DataFrame(
        columns=[
            'channel',
            'message_id',
            'text',
            'datetime',
            'reply_to',
        ]
    )

    for item in tqdm(config['channels']):
        offset_date = (
            pytz.UTC.localize(datetime.fromtimestamp(item['end']))
        ) if 'end' in item.keys() else None

        async for message in client.iter_messages(
            item['name'],
            offset_date=offset_date
        ):
            if message.date < pytz.UTC.localize(
                datetime.fromtimestamp(item['start'])
            ):
                break
            reply_to = await message.get_reply_message()
            new_row = pd.DataFrame({
                'channel': [item['name']],
                'message_id': [message.id],
                'text': [message.text],
                'datetime': [message.date],
                'reply_to': [reply_to.id] if reply_to else [None]
            })
            messages_data_frame = pd.concat(
                [messages_data_frame, new_row],
                ignore_index=True,
                axis=0
            )

    return messages_data_frame


def main():
    with open('credentials.json', 'r') as credentials_fd:
        credentials = json.load(credentials_fd)

    with open('config.json', 'r') as config_fd:
        config = json.load(config_fd)

    with TelegramClient(
        'user',
        credentials['api_id'],
        credentials['api_hash']
    ) as client:
        messages_data_frame = client.loop.run_until_complete(
            gather_messages(client, config)
        )

    messages_data_frame.to_csv(config['output'], index=False)


if __name__ == '__main__':
    main()
