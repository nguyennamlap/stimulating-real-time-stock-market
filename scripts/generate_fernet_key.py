from cryptography.fernet import Fernet

def generate_key():
    key = Fernet.generate_key().decode()
    print("Key là: ")
    print(key)

if __name__ == "__main__":
    generate_key()