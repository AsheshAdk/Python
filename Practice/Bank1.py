# Class as account
class Account:
    bank_name = "Nabil Bank"  # class variable

    def __init__(self, name, pin, balance=0):
        self.name = name
        self.__pin = pin       # private PIN
        self.balance = balance

    # deposit doesn't need PIN
    def deposit(self, amount):
        if amount <= 0:
            print("Amount must be greater than 0")
            return
        self.balance += amount
        print(f"{self.name} deposited NPR {amount}. New balance: {self.balance}")

    # withdraw needs a PIN
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

    # transfer needs a PIN
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

    # view balance
    def show_balance(self):
        print(f"{self.name}'s balance: NPR {self.balance}")


# test block
if __name__ == "__main__":
    print(f"Welcome to {Account.bank_name}!\n")

    # create account with name, pin, and balance
    staff = Account("Ashesh", 1234, 1000)
    ram = Account("Krishna", 1111, 500)

    # staff account operations
    staff.deposit(200)
    staff.withdraw(150, 1234)
    staff.transfer(300, ram, 1234)

    print()

    # ram account operations
    ram.deposit(100)
    ram.withdraw(50, 1111)
    ram.transfer(200, staff, 9999)  # wrong PIN

    print()

    # final balances
    staff.show_balance()
    ram.show_balance()