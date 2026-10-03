# Customer screen: what any front end must keep doing

The back end is already built and tested. A new look (hand-written, or built in Claude Code with React)
connects to it again as long as it keeps this contract. Nothing else in the project needs to change.

Back end: `python m4_app.py`, serves http://127.0.0.1:8000. Static page: `web/index.html`.
Pipeline behind it: Kafka, `m3_processor.py` (recommendations), MongoDB.

## 1. Endpoints

| Call | Purpose | Returns |
| --- | --- | --- |
| `GET /api/config` | Which customers exist, free-delivery threshold, categories | `{"demo_users": {"treatment": 75482, "control": 31826}, "groups": {"75482": "treatment", ...}, "free_delivery_threshold": 999, "categories": [...]}` |
| `GET /api/products?category=&q=&limit=` | Product catalogue (limit up to 300) | list of `{product_id, name, brand, category, price, offer_pct, offer_name, rating (number or null), rating_count}` |
| `POST /api/action` | Tell the pipeline what the customer did | `{"event_ids": [...]}` (HTTP 400 on a bad request) |
| `WebSocket /ws/{user_id}` | Receive live recommendations | one JSON message per recommendation (section 3) |

The customer id comes from the URL: `?group=treatment` (live recommendations) or `?group=control` (stale). Both must keep working.

## 2. Actions to send: `POST /api/action`

Body always has `user_id`, `session_id` (one random id per page load) and `type`.

| When the customer... | type | Extra fields |
| --- | --- | --- |
| opens the page (after the WebSocket is connected) | `app_open` | none |
| clicks a product card | `product_click` | `product_id` |
| adds one unit | `add_to_cart` | `product_id`, `qty: 1` |
| removes a line | `remove_from_cart` | `product_id`, `qty` (the whole line quantity) |
| clicks a recommendation card | `rec_click` | `rec_id`, `product_id`, `rule` |
| adds a recommended item | `rec_add` | `rec_id`, `product_id`, `rule` |
| places the order | `order` | `items: [{product_id, qty}]` (the server works out prices and promotions) |

Why it matters: click-through by rule on the dashboard only works if `rec_id` and `rule` are sent with every recommendation click.
A stepper that goes from 1 to 2 sends another `add_to_cart` with `qty: 1`; going down sends `remove_from_cart` with `qty: 1`.

## 3. Recommendation message (WebSocket)

```json
{"type": "recommendation", "rec_id": "r_ab12", "user_id": 75482, "group": "treatment",
 "view": "live", "trigger": "add_to_cart", "trigger_event_id": "e_9f...", "latency_ms": 22.5,
 "cart_value": 599.0,
 "recommendations": [
   {"product_id": 123, "name": "Biscuits", "brand": "...", "category": "Snacks & Munchies", "price": 137.0,
    "rating": 3.8, "rating_count": 21, "offer_pct": 15, "offer_name": "Flash Sale",
    "rule": "cart_complement", "reason": "Frequently bought with Milk", "rating_adjusted": false}]}
```

* `view` is `"live"` or `"stale"`. For a stale customer the list is often empty: show a short note, not an error.
* `rule` is one of `cart_complement`, `offer`, `trending`. Show `reason` on the card: it is the explainable part of the project.
* Latency shown to the audience: remember the time each action was posted and keep the `event_ids` returned. When a message arrives whose `trigger_event_id` is one of them, show the elapsed milliseconds.
* Reconnect the WebSocket if it drops.

## 4. Rules the page must respect

* The cart lives in the browser. The server rebuilds its own copy from the events, and `app_open` starts a fresh cart there.
* Never send prices: the server uses the catalogue price and the category promotion (`offer_pct`, applied at checkout).
* Free delivery above `free_delivery_threshold` (Rs 999) after promotions. Below it a delivery fee can be *shown* (for example Rs 25 plus Rs 2 handling) but the pipeline does not record fees, so order values stay item totals.
* Search and category filter only call `/api/products`.

## 5. Layout ideas taken from the Blinkit screenshots (layout only)

| In the screenshots | In our screen | Data we have |
| --- | --- | --- |
| Header with delivery promise, location, search, cart button with item count and total | Same, with our own name (QuickMart) and a fixed "9 mins" label | none needed |
| Category rail on the left | Category list from `/api/config` | `categories` |
| Product card grid: image, discount badge, pack size, price with struck-through old price, ADD turning into - 1 + | Same card layout. Badge from `offer_pct`. No struck-through old price: the API does not send an old price and the page must not invent one | price, offer_pct, rating |
| Cart drawer: items, Bill details (items total, saved, delivery charge, handling charge, grand total), checkout button | Same, with our free-delivery progress bar | cart is local |
| "You might also like" rows | The Recommended for you strip, each card with its reason chip | WebSocket message |

Do not copy: the Blinkit name or logo, its product photos, its banners or ads. The dataset has no product images, so use neutral placeholder tiles (category colour plus the product name).

## 6. Hand-off to Claude Code

Before you start (Windows, in the `qc` folder): keep a copy of the working page.

    copy web\index.html web\index_old.html

Files to have in the `qc` folder: `UI_CONTRACT.md` and `m4_ui_dev.py` (a copy of the customer screen that needs no Kafka and no MongoDB).

**Paste this into Claude Code** (open it in VS Code with the `qc` folder open):

> Read `UI_CONTRACT.md`, `web/index.html` and `m4_app.py` first.
> Redesign `web/index.html` as a quick-commerce grocery screen: header with search and a cart button (item count and total),
> a left category rail, a product card grid (placeholder tile, name, brand, rating, price, a discount badge from `offer_pct`,
> ADD turning into a - 1 + stepper), a slide-over cart with bill details and the free-delivery progress bar, and a
> "Recommended for you" strip whose cards show the reason chip.
> Rules:
> 1. Edit ONLY `web/index.html`. Put all CSS and JavaScript inside that one file (the server serves only that file).
>    Do not change any Python file and do not add other files.
> 2. Keep every call in `UI_CONTRACT.md` exactly as written: endpoints, `POST /api/action` types and fields
>    (including `rec_id` and `rule` on recommendation clicks), the WebSocket message and the latency display.
> 3. No photos, no logo, no ads, no struck-through old prices. Use CSS placeholder tiles. The shop is called QuickMart.
> 4. Test with `python m4_ui_dev.py`, which needs no Kafka, and open `http://127.0.0.1:8000/?group=treatment`
>    and `?group=control`. Check: adding Milk shows 3 recommendations with reasons for the treatment customer,
>    the control customer shows the stale-data note, Place order clears the cart.
> Stop after one working version and tell me what you could not check.

**Why it connects back without any extra step:** the page is served by `m4_app.py` at the same address as the API and the WebSocket,
so it talks to them with relative URLs. Every action it posts becomes the same Kafka event as before, so the processor, MongoDB and
the dashboard do not change. `m4_ui_dev.py` serves the same page and API with the real recommender but no Kafka.

**After it is done (you, not Claude Code):**
1. Start the full pipeline as usual (Kafka, consumer, processor, producer) and run `python m4_app.py`.
2. Open the page, add an item, click Add on a recommendation, place an order.
3. On the dashboard: the funnel moves, and the Rule performance table shows the rule you clicked.
4. If anything is wrong: copy `web\index_old.html` back over `web\index.html`.
