import asyncio
from typing import Dict
import sys
import os

# I hate this, but it's how I got the import to actually work
current_dir = os.path.dirname(os.path.realpath(__file__))
base_dir = os.path.join(current_dir, "../")
base_dir = os.path.abspath(base_dir)
sys.path.append(base_dir)

from framework.message import Message
from framework.listener import MessageListener
from framework.dispatcher import Dispatcher

"""
This program does pretty much the same thing as the one in card_dealer.py, but it demonstrates the use
of synchronous messaging.
"""


class CardPlayer(MessageListener):

    def __init__(self, name, is_dealer):
        super().__init__()
        self._id = name
        self._is_dealer = is_dealer
        self._player_ids = []
        self.filtering_table.add_entry({"message_type": "Greeting"}, callback=self.handle_greeting)
        self.filtering_table.add_entry({"message_type": "Card"})

    async def dealing_round(self):
        """Simulates player participation in a round of dealing"""
        dispatcher = Dispatcher.get_instance()
        await dispatcher.register_listener(self)
        await dispatcher.register_listener_in_group(self, "All")

        # First, register players by sending greeting to whole group
        dispatcher.send_message_sync(message_type="Greeting", source_id=self._id, group_id="All",
                                      content={"dealer": self._is_dealer})

        if self._is_dealer:
            # Wait for player greetings to arrive
            while len(self._player_ids) < 4:
                await asyncio.sleep(0.001)
            self._player_ids.sort()
            player_counter = 1
            # Deal out twenty cards, rotating between players
            for i in range(20):
                dispatcher.send_message_sync(message_type="Card", source_id=self._id,
                                              destination_id=self._player_ids[player_counter],
                                              content={"card": i})
                player_counter = (player_counter + 1) if player_counter < 3 else 0

        # Wait for expected card messages
        cards = []
        while len(cards) < 5:
            msg = await self.get_queued_message()
            card = msg.content["card"]
            print(f"Player {self._id} got card", card)
            cards.append(card)

    def handle_greeting(self, message: Dict):
        """Callback that receives 'Greeting' message from listener"""
        print(f"Player {self._id} got greeting from player {message['source_id']}")
        if self._is_dealer:
            self._player_ids.append(message["source_id"])


async def main():
    """Make four players and get dealing round going"""
    players = []
    tasks = []
    for i in range(4):
        player = CardPlayer("player" + str(i + 1), is_dealer=(i == 0))
        players.append(player)
        task = asyncio.create_task(player.dealing_round())
        tasks.append(task)

    while True:
        done_count = 0
        for task in tasks:
            if task.done():
                done_count += 1
        if done_count == 4:
            break
        await asyncio.sleep(0.01)


asyncio.run(main())
