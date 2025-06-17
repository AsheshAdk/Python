# Class as account
class Account:
    bank_name = "Nabil Bank"

    def __init__(self, name, pin, balance=0):
        self.name = name
        self.__pin = pin        # private
        self.balance = balance

    # Deposit never needs a PIN so there is no need of PIN
    def deposit(self, amount):
        if amount <= 0:
            print("Amount must be greater than 0")
            return
        self.balance += amount
        print(f"{self.name} deposited NPR {amount}. New balance: {self.balance}")

    # Withdraw needs a PIN
    def withdraw(self, amount, pin):
        if amount <= 0:
            print("Amount must be greater than 0")
        elif pin != self.__pin:
            print("Incorrect PIN")
        elif amount > self.balance:
            print("Not enough balance")
        else:
            self.balance -= amount
            print(f"{self.name} withdrew NPR {amount}. Balance: {self.balance}")

    # Transfer needs a PIN
    def transfer(self, amount, receiver, pin):
        if amount <= 0:
            print("Amount must be greater than 0")
        elif pin != self.__pin:
            print("Incorrect PIN")
        elif amount > self.balance:
            print("Not enough balance")
        else:
            self.balance -= amount
            receiver.balance += amount
            print(f"{self.name} sent NPR {amount} to {receiver.name}. Your balance: {self.balance}")

    def show_balance(self):
        print(f"{self.name}'s balance: NPR {self.balance}")


# test though example
if __name__ == "__main__":
    print(f"Welcome to {Account.bank_name}!\n")

    staff = Account("Ashesh", 1234, 1000)   # staff account
    ram   = Account("Ram",    1111,  500)   # customer account

    staff.deposit(200)
    staff.withdraw(150, 1234)          # valid PIN
    staff.transfer(300, ram, 1234)     # valid transfer

    print()
    ram.deposit(100)
    ram.withdraw(50, 1111)             # valid
    ram.transfer(200, staff, 9999)     # wrong PIN

    print()
    staff.show_balance()
    ram.show_balance()

    Account.bank_name = "Nabil Nepal"
    print("\n New bank name:", Account.bank_name)