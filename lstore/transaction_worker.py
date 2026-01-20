import threading
import time
from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.thread = None  # Store the worker's thread
    
    """
    Append t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)
        
    """
    Run all transaction as a thread
    """
    def run(self):
        # Create a thread that executes __run
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()
    
    """
    Wait for the worker to finish
    """
    def join(self):
        if self.thread:
            self.thread.join()

    def __run(self):
        MAX_RETRIES = 100
        
        # Process each transaction in the batch
        for transaction in self.transactions:
            retry_count = 0
            
            # Retry loop with exponential backoff
            while retry_count < MAX_RETRIES:
                result = transaction.run()
                
                # Transaction succeeded
                if result:
                    self.stats.append(True)
                    self.result += 1
                    break
                
                # Transaction aborted - backoff before retry
                retry_count += 1
                delay = 0.001 * min(retry_count, 10)  # 10ms delay
                jitter = delay * 0.5 * (hash(threading.current_thread()) % 100) / 100
                time.sleep(delay + jitter)
            
            # Max retries exceeded
            else:
                self.stats.append(False)
