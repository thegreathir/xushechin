import json
from telethon import TelegramClient

async def gather_messages(client: TelegramClient):
    with open('config.json', 'r') as config_fd:
        config = json.load(config_fd)

    for channel in config['channels']:
        print(f"##### {channel} #####")
        async for message in client.iter_messages(
            channel,
            limit=config['message_count']
        ):
            print(message.text)


def main():
    with open('credentials.json', 'r') as credentials_fd:
        credentials = json.load(credentials_fd)

    with TelegramClient(
        'user',
        credentials['api_id'],
        credentials['api_hash']
    ) as client:
        client.loop.run_until_complete(gather_messages(client))

if __name__=='__main__':
    main()