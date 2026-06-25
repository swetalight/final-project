#!/usr/bin/env python3
"""
CLI для управления списком запрещённых товаров

Команды:
- add <id> — добавить товар в список
- remove <id> — удалить товар из списка
- list — показать все запрещённые товары
- exit — выход
"""

import json
import os
from dotenv import load_dotenv

# Загружаем .env
load_dotenv()


class BannedCLI:
    def __init__(self, banned_file="data/banned_products.json"):
        self.banned_file = banned_file
        self.banned = self._load()

    def _load(self):
        if os.path.exists(self.banned_file):
            with open(self.banned_file, "r", encoding="utf-8") as f:
                return set(json.load(f))
        return set()

    def _save(self):
        with open(self.banned_file, "w", encoding="utf-8") as f:
            json.dump(list(self.banned), f, indent=2, ensure_ascii=False)

    def add(self, product_id):
        self.banned.add(product_id)
        self._save()
        print(f"✅ Товар {product_id} добавлен в список запрещённых")

    def remove(self, product_id):
        if product_id in self.banned:
            self.banned.remove(product_id)
            self._save()
            print(f"✅ Товар {product_id} удалён из списка запрещённых")
        else:
            print(f"❌ Товар {product_id} не найден в списке")

    def list_products(self):
        print("\n📋 Запрещённые товары:")
        if self.banned:
            for pid in sorted(self.banned):
                print(f"  - {pid}")
        else:
            print("  (список пуст)")


if __name__ == "__main__":
    cli = BannedCLI()

    print("\n🔐 Управление списком запрещённых товаров")
    print("Команды: add <id> | remove <id> | list | exit")

    while True:
        cmd = input("\n> ").strip()
        if cmd == "exit":
            break
        elif cmd == "list":
            cli.list_products()
        elif cmd.startswith("add "):
            cli.add(cmd[4:].strip())
        elif cmd.startswith("remove "):
            cli.remove(cmd[7:].strip())
        else:
            print("❌ Неизвестная команда. Используйте: add, remove, list, exit")