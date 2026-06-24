#!/usr/bin/env python3
"""CLI для управления списком запрещённых товаров"""

import json
import os


class BannedCLI:
    def __init__(self, banned_file="data/banned_products.json"):
        self.banned_file = banned_file
        self.banned = self._load()

    def _load(self):
        if os.path.exists(self.banned_file):
            with open(self.banned_file, "r") as f:
                return set(json.load(f))
        return set()

    def _save(self):
        with open(self.banned_file, "w") as f:
            json.dump(list(self.banned), f, indent=2)

    def add(self, product_id):
        self.banned.add(product_id)
        self._save()
        print(f"✅ Товар {product_id} добавлен в список")

    def remove(self, product_id):
        self.banned.discard(product_id)
        self._save()
        print(f"✅ Товар {product_id} удалён из списка")

    def list_products(self):
        print("📋 Запрещённые товары:")
        for pid in sorted(self.banned):
            print(f"  - {pid}")


if __name__ == "__main__":
    cli = BannedCLI()
    while True:
        cmd = input("\n📌 (add <id> / remove <id> / list / exit): ")
        if cmd == "exit":
            break
        elif cmd == "list":
            cli.list_products()
        elif cmd.startswith("add "):
            cli.add(cmd[4:])
        elif cmd.startswith("remove "):
            cli.remove(cmd[7:])
        else:
            print("❌ Неизвестная команда")