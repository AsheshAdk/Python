# function will be outside class
def welcome_message():
    print("Welcome to Zakipoint Office")

# Call the function
welcome_message()

# Class named as Office
class Office:
    def __init__(self, name, location, employees):
        # Attributes (variables inside objects)
        self.name = name
        self.location = location
        self.employees = employees  # number of employees

    # Method
    def show_info(self):
        print(f"{self.name} is located in {self.location}.")
        print(f"It has {self.employees} employees.")

    # Method
    def open_office(self):
        print(f"{self.name} office in {self.location} is now OPEN.")

    # Method
    def close_office(self):
        print(f"{self.name} office in {self.location} is now CLOSED for today.")

# for creating office as objects
office1 = Office("Zakipoint", "Kathmandu, Nepal", 25)
office2 = Office("Zakipoint", "Pokhara, Nepal", 15)

# uses of objects and methods
office1.show_info()
office1.open_office()
office2.show_info()
office2.open_office()
office2.close_office()
