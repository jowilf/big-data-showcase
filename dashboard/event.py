from datetime import datetime
import random
from plotly.graph_objs.scatter import Legendgrouptitle
import plotly.graph_objs as go

import json
from collections import deque


class RealTimeEvent:
    """
    Class to store and visualize real-time user behavior data.

    Attributes:
    -----------
    dates : list of datetime.datetime
        List of datetime objects representing the timestamp of each data point.
    views : list of int
        List of integers representing the number of views at each timestamp.
    purchases : list of int
        List of integers representing the number of purchases at each timestamp.
    carts : list of int
        List of integers representing the number of items added to cart at each timestamp.
    queue : collections.deque
        A deque to store incoming events.
    """

    def __init__(self) -> None:
        """
        Initializes the RealTimeEvent object with empty lists for dates, views, purchases and carts,
        and an empty deque for incoming events.
        """
        self.dates: list[datetime] = []
        self.views: list[int] = []
        self.purchases: list[int] = []
        self.carts: list[int] = []
        self.queue: deque[dict] = deque()

    def append(self, data: dict):
        """
        Appends the views, purchases and carts data from the input dictionary to their respective lists,
        and appends the current datetime to the dates list. If the length of the lists exceeds 15, the oldest
        element is removed from each list.

        Parameters:
        -----------
        data : dict
            A dictionary with keys 'view', 'purchase' and 'cart', and integer values representing the
            number of views, purchases and carts in the incoming event.
        """
        self.dates.append(datetime.now())
        self.views.append(data.get("view", 0))
        self.purchases.append(data.get("purchase", 0))
        self.carts.append(data.get("cart", 0))
        if len(self.dates) > 15:
            self.pop()

    def pop(self, idx=0):
        """
        Removes an element from each of the lists of dates, views, purchases and carts.

        Parameters:
        -----------
        idx : int, default 0
            The index of the element to be removed from the lists.
        """
        self.dates.pop(idx)
        self.views.pop(idx)
        self.purchases.pop(idx)
        self.carts.pop(idx)

    def append_random(self):
        """
        Appends random data to the views, purchases and carts lists using the append method.
        Only for testing purpose
        """
        self.append(
            {
                "view": random.randint(0, 20),
                "purchase": random.randint(0, 20),
                "cart": random.randint(0, 20),
            }
        )

    def add_event(self, value):
        """
        Adds an incoming event to the deque of events to be processed.

        Parameters:
        -----------
        value : str
            A JSON string with the incoming event data.
        """
        self.queue.append(value)

    def next(self):
        """
        Processes the next event in the buffer.
        """
        if len(self.queue) > 0:
            value = self.queue.popleft()
            data = json.loads(value)
            if len(data) > 0:
                self.append(data)

    def figure(self):
        """
        Generates a Plotly Figure object representing the stream data.

        Returns:
            A Plotly Figure object.
        """
        return go.Figure(
            data=[
                go.Scatter(
                    x=self.dates,
                    y=self.views,
                    legendgroup="view",
                    name="View",
                    legendgrouptitle=Legendgrouptitle(text="View"),
                ),
                go.Scatter(
                    x=self.dates,
                    y=self.carts,
                    name="Cart",
                    legendgroup="cart",
                    legendgrouptitle=Legendgrouptitle(text="Cart"),
                ),
                go.Scatter(
                    x=self.dates,
                    y=self.purchases,
                    name="Purchase",
                    legendgroup="purchase",
                    legendgrouptitle=Legendgrouptitle(text="Purchase"),
                ),
            ],
            layout=go.Layout(title="Real Time user behavior in the store"),
        )
