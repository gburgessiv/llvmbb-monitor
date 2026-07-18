# Privacy Policy: LLVM Buildbot Monitor

**Effective Date:** July 17, 2026

The **LLVM Buildbot Monitor** (referred to as "the Bot", "we", "us", or "our") is a Discord bot designed to monitor LLVM buildbots and public community calendars. It pings relevant developers when buildbots fail and notifies users of upcoming community events.

This Privacy Policy explains how the Bot collects, processes, stores, and protects user data.

---

## 1. Data We Access

To perform its core functions, the Bot requires access to specific information about the server and its members. We only request and access the minimum data necessary to run the service.

### A. Server Member Information
* **Why we need it**: The Bot maps Git commit email addresses of failing builds to Discord users so it can alert the correct developers. We check if you are a member of the Discord server before attempting to ping you, and use your nickname or username to format notifications clearly.
* **What we access**: The list of server members, including **Discord User IDs**, **Usernames**, and **Server Nicknames**.
* **How we handle it**:
  * This data is queried dynamically and stored temporarily in memory (cached for up to 1 hour).
  * This member information is never written to persistent databases or long-term storage.
  * If you leave the server, your information will be removed from this temporary cache when the 1-hour time-to-live expires.

### B. Direct Messages
* **Why we need it**: The Bot supports commands in Direct Messages (DMs) so developers can securely associate their email addresses with their Discord account without exposing their email to the public server.
* **What we access**: The text content of Direct Messages sent directly to the Bot.
* **How we handle it**:
  * We only process message content to respond to setup commands (such as listing, adding, or removing your email).
  * With the exception of the dedicated status channel (detailed below), the Bot does not read or process message content from public server channels.

### C. Status Channel Messages
* **Why we need it**: The Bot maintains a dedicated status channel with persistent messages that update automatically. To keep this channel clean, the Bot reads the channel history to find and delete any messages not authored by itself.
* **What we access**: Message metadata (such as the author ID) of messages posted in the dedicated status channel.
* **How we handle it**:
  * We only process this data to determine if a message was sent by the Bot or by another user.
  * If a message in the status channel was not sent by the Bot, it is automatically deleted. We do not store or log the content of these deleted messages.


---

## 2. Persistent Data Storage

We do not collect or store any information automatically. The only persistent data stored is explicitly provided by you through Direct Messages:

* **Email Mappings**: If you associate your Git commit email with your Discord account, the mapping between your Discord User ID and the email address is stored securely on our server.
* **Calendar Event Identifiers**: We track public calendar event IDs to avoid sending duplicate notifications, but this data contains no personal information.

---

## 3. Data Control, Opt-Out, and Deletion (Your Rights)

You have full control over the personal data we store. You can manage your information at any time via Direct Messages with the Bot:

* **View Mapped Data**: Send `list-emails` to the Bot in a DM to see all email addresses currently associated with your Discord account.
* **Opt-In/Add Mappings**: Send `add-email <email-address>` to map an email.
* **Opt-Out/Remove Mappings**: Send `rm-email <email-address>` to completely delete the association. Removing all mappings deletes your data record entirely.
* **Complete Erasure**: If you remove your email associations, your stored mapping is deleted. For manual erasure requests, contact the bot maintainers at the project's [GitHub repository](https://github.com/gburgessiv/llvmbb-monitor).

---

## 4. Third-Party Sharing

* We do not sell, trade, or share any personal user data with third parties.
* Data is only shared with the **Discord API** as required to transmit notifications and process commands.
* We read public data from LLVM build systems and Google Calendar APIs, but no user IDs or email mappings are ever sent to these external services.
