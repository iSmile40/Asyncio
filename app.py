import sqlite3
import aiosmtplib
from email.message import EmailMessage
import asyncio
from more_itertools import chunked


async def send_to_contacts(contact: tuple):
    message = EmailMessage()
    message["From"] = "*****"
    message["To"] = contact[3]
    message["Subject"] = "Здравствуйте"
    message.set_content(f"Уважаемый {contact[1]} {contact[2]}\nСпасибо, что пользуетесь нашим сервисом объявлений.")
    await aiosmtplib.send(message,
                          hostname="smtp.yandex.ru",
                          port=465,
                          use_tls=True,
                          username="*****",
                          password="*****")

async def main():
    filename = "contacts.db"
    con = sqlite3.connect(filename)
    cur = con.cursor()
    cur.execute("SELECT * FROM contacts")
    contacts = cur.fetchall()
    for chunk in chunked(contacts, 100):
        tasks = [asyncio.create_task(send_to_contacts(contact)) for contact in chunk]
        return await asyncio.gather(*tasks)
    cur.close()


event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(main())
