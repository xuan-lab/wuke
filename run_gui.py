import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext
import subprocess
import threading
import sys
import os
from pathlib import Path

# --- Configuration ---
DEFAULT_HERB_LIST = Path(__file__).parent / 'herb_list.txt'
DEFAULT_TCMBANK = Path(__file__).parent / 'data' / 'tcmbank.xlsx'
RUNNER_SCRIPT = Path(__file__).parent / 'run.py'
SRC_DIR = Path(__file__).parent / 'src' # For context

class WorkflowGUI:
    def __init__(self, master):
        self.master = master
        master.title("WUKE  Workflow")
        master.geometry("700x550")

        # --- Variables ---
        self.herb_list_path = tk.StringVar(value=str(DEFAULT_HERB_LIST))
        self.tcmbank_path = tk.StringVar(value=str(DEFAULT_TCMBANK))
        self.running = False
        self.process = None

        # --- Style ---
        style = ttk.Style()
        style.configure("TButton", padding=6, relief="flat", background="#ccc")
        style.configure("TLabel", padding=5)
        style.configure("TEntry", padding=5)

        # --- Layout ---
        main_frame = ttk.Frame(master, padding="10 10 10 10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- File Selection ---
        file_frame = ttk.LabelFrame(main_frame, text="Input Files", padding="10")
        file_frame.pack(fill=tk.X, pady=5)

        # Herb List
        ttk.Label(file_frame, text="Herb List File:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(file_frame, textvariable=self.herb_list_path, width=60).grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(file_frame, text="Browse...", command=self.browse_herb_list).grid(row=0, column=2, padx=5)

        # TCMSP Bank
        ttk.Label(file_frame, text="TCMSP Bank File:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(file_frame, textvariable=self.tcmbank_path, width=60).grid(row=1, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(file_frame, text="Browse...", command=self.browse_tcmbank).grid(row=1, column=2, padx=5)

        file_frame.columnconfigure(1, weight=1) # Make entry expand

        # --- Controls ---
        control_frame = ttk.Frame(main_frame, padding="5")
        control_frame.pack(fill=tk.X, pady=5)

        self.run_button = ttk.Button(control_frame, text="Run Workflow", command=self.start_workflow)
        self.run_button.pack(side=tk.LEFT, padx=5)

        self.stop_button = ttk.Button(control_frame, text="Stop Workflow", command=self.stop_workflow, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        # --- Status/Log Output ---
        log_frame = ttk.LabelFrame(main_frame, text="Status / Log", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, height=20, width=80, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # --- Initial Check ---
        if not RUNNER_SCRIPT.exists():
             self.log_message(f"ERROR: Runner script not found at {RUNNER_SCRIPT}\n", "error")
             self.run_button.config(state=tk.DISABLED)
        if not Path(sys.executable).exists():
             self.log_message(f"ERROR: Python executable not found at {sys.executable}\n", "error")
             self.run_button.config(state=tk.DISABLED)


    def browse_herb_list(self):
        filepath = filedialog.askopenfilename(
            title="Select Herb List File",
            initialdir=Path(__file__).parent,
            filetypes=(("Text files", "*.txt"), ("All files", "*.*"))
        )
        if filepath:
            self.herb_list_path.set(filepath)

    def browse_tcmbank(self):
        filepath = filedialog.askopenfilename(
            title="Select TCMSP Bank File",
            initialdir=Path(__file__).parent / 'data',
            filetypes=(("Excel files", "*.xlsx"), ("All files", "*.*"))
        )
        if filepath:
            self.tcmbank_path.set(filepath)

    def log_message(self, message, tag=None):
        """Appends a message to the log text area."""
        self.log_text.config(state=tk.NORMAL)
        if tag:
            self.log_text.insert(tk.END, message, (tag,))
        else:
            self.log_text.insert(tk.END, message)
        self.log_text.see(tk.END) # Scroll to the end
        self.log_text.config(state=tk.DISABLED)
        self.master.update_idletasks() # Ensure GUI updates

    def set_running_state(self, is_running):
        self.running = is_running
        self.run_button.config(state=tk.DISABLED if is_running else tk.NORMAL)
        self.stop_button.config(state=tk.NORMAL if is_running else tk.DISABLED)

    def start_workflow(self):
        herb_list = self.herb_list_path.get()
        tcmbank = self.tcmbank_path.get()

        if not Path(herb_list).is_file():
            self.log_message(f"ERROR: Herb list file not found: {herb_list}\n", "error")
            return
        if not Path(tcmbank).is_file():
            self.log_message(f"ERROR: TCMSP Bank file not found: {tcmbank}\n", "error")
            return
        if not RUNNER_SCRIPT.is_file():
             self.log_message(f"ERROR: Runner script not found: {RUNNER_SCRIPT}\n", "error")
             return

        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete('1.0', tk.END) # Clear previous logs
        self.log_text.config(state=tk.DISABLED)

        self.log_message("Starting workflow...\n")
        self.set_running_state(True)

        # Run the workflow in a separate thread
        self.thread = threading.Thread(target=self.run_workflow_thread, args=(herb_list, tcmbank), daemon=True)
        self.thread.start()

    def stop_workflow(self):
        if self.process and self.running:
            self.log_message("\n--- Sending stop signal... ---\n", "warning")
            try:
                # Terminate the process group on Unix-like, or the process itself on Windows
                if os.name == 'nt':
                    subprocess.run(['taskkill', '/F', '/T', '/PID', str(self.process.pid)], check=False, capture_output=True)
                else:
                    os.killpg(os.getpgid(self.process.pid), signal.SIGTERM) # More forceful
                self.log_message("--- Stop signal sent. Process may take time to exit. ---\n", "warning")
            except Exception as e:
                self.log_message(f"--- Error sending stop signal: {e} ---\n", "error")
            # Let the thread finish naturally after process termination
            # self.set_running_state(False) # State will be reset when thread finishes

    def run_workflow_thread(self, herb_list, tcmbank):
        """Runs the run.py script in a subprocess and streams output."""
        try:
            command = [sys.executable, str(RUNNER_SCRIPT), herb_list, tcmbank]
            # Use preexec_fn=os.setsid on Unix-like systems to allow killing the whole process group
            start_new_session = True if os.name != 'nt' else False

            self.process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1,  # Line buffered
                universal_newlines=True,
                # start_new_session=start_new_session # Creates new process group on Unix
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0 # For taskkill /T
            )

            # Stream output line by line
            for line in iter(self.process.stdout.readline, ''):
                self.master.after(0, self.log_message, line) # Schedule GUI update from main thread

            self.process.wait() # Wait for the process to complete
            self.master.after(0, self.log_message, f"\n--- Workflow process finished with exit code: {self.process.returncode} ---\n")

        except FileNotFoundError:
             self.master.after(0, self.log_message, f"ERROR: Python or runner script not found.\nPython: {sys.executable}\nScript: {RUNNER_SCRIPT}\n", "error")
        except Exception as e:
            self.master.after(0, self.log_message, f"ERROR: Failed to run workflow: {e}\n", "error")
        finally:
            self.process = None
            self.master.after(0, self.set_running_state, False) # Ensure GUI state is updated from main thread


if __name__ == "__main__":
    root = tk.Tk()
    # Configure tags for coloring messages
    log_text_widget = getattr(WorkflowGUI(root), 'log_text', None) # Access widget after init
    if log_text_widget:
        log_text_widget.tag_config("error", foreground="red")
        log_text_widget.tag_config("warning", foreground="orange")
        log_text_widget.tag_config("info", foreground="blue") # Example

    root.mainloop()
