import subprocess

tests_to_run = [
    "blocking_message_example.py",
    "card_dealer.py",
    "card_dealer_sync.py",
    "deferred_reply_example.py",
    "robustness_test.py",
]

for test_file in tests_to_run:
    print(f"Running test {test_file}...")
    print("=====================================================================\n")
    with open(test_file) as f:
        subprocess.run(["python3", test_file])
    print("")
