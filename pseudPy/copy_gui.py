import os
import tkinter as tk
import polars as pl
from tkinter import messagebox
from tkinter import font as tkFont

import Pseudonymization as pseudPy
import polars.exceptions
from tkinter import filedialog


def initialize_pseudonym():
    is_structured = True
    map_columns_list = []
    map_method = map_method_var.get()
    if structure_var.get() == "structured":
        map_columns = map_columns_entry.get()
        if map_columns == "-":
            map_columns = None
        else:
            map_columns_list = map_columns.split(",")
    input_file = input_file_entry.get()
    if not os.path.exists(input_file):
        messagebox.showinfo("Failed", "Input path does not exist!")
    if structure_var.get() == "free text":
        pos_type = pos_type_var.get()
        if pos_type == "-":
            pos_type = None
        patterns = patterns_entry.get()
        if patterns == "-":
            patterns = None
        else:
            patterns = modify_pattern(patterns)
        all_ne = all_ne_var.get()
    output = output_entry.get()
    if not os.path.exists(output):
        messagebox.showinfo("Failed", "Output path does not exist!")
    mapping = mapping_var.get()
    encrypt_map = encrypt_map_var.get()
    seed = seed_entry.get()
    if seed == "-":
        seed = None

    try:
        pl.read_csv(input_file)
        print("The data is structured.")
    except polars.exceptions.ComputeError:
        is_structured = False
        print("The data is not structured.")
    except FileNotFoundError:
        messagebox.showinfo("Failed", "Please fill out the form first!")

    if map_method == 'decrypt':
        if is_structured:
            df = pl.read_csv(input_file)
            for elem in map_columns_list:
                mapping = pseudPy.Mapping(df, first_tier=elem, output=output)
                decrypt = pseudPy.Mapping.decrypt_tier(mapping)
                decrypt = decrypt.rename(f"{elem}")
                df = df.drop(elem)
                df = df.insert_column(0, decrypt)
            df.write_csv(f"{output}/decrypted_output.csv")
    else:
        if is_structured:
            pseudo = pseudPy.Pseudonymization(
                map_columns=map_columns_list,
                map_method=map_method,
                input_file=input_file,
                output=output,
                mapping=mapping,
                encrypt_map=encrypt_map,
                seed=seed
            )
            pseudo.pseudonym()
            messagebox.showinfo("Success", "Pseudonymization successful!")
        else:
            pseudo = pseudPy.Pseudonymization(
                map_method=map_method,
                input_file=input_file,
                output=output,
                mapping=mapping,
                encrypt_map=encrypt_map,
                seed=seed,
                pos_type=pos_type,
                patterns=patterns,
                all_ne=all_ne
            )
            pseudo.nlp_pseudonym()
            messagebox.showinfo("Success", "Pseudonymization successful!")


def revert_data():
    is_structured = True
    input_mapping = input_mapping_entry.get()
    if not os.path.exists(input_mapping):
        messagebox.showinfo("Failed", "Mapping path does not exist!")
    input_file_revert = input_file_entry_revert.get()
    if not os.path.exists(input_file_revert):
        messagebox.showinfo("Failed", "Input path does not exist!")
    output_revert = output_entry_revert.get()
    if not os.path.exists(output_revert):
        messagebox.showinfo("Failed", "Output path does not exist!")

    try:
        pl.read_csv(input_file_revert)
    except polars.exceptions.ComputeError:
        is_structured = False

    revert_df = pl.read_csv(input_mapping)
    columns = map_columns_entry_revert.get()
    if not columns or (columns == "-"):
        messagebox.showinfo("Failed", "Please specify the column to map!")

    if is_structured:
        df = pl.read_csv(input_file_revert)
        if pseudonyms_entry_revert.get() != "-":
            pseudonyms = pseudonyms_entry_revert.get().split(",")
            try:
                revert_df = revert_df.filter(pl.col(f"Index_{columns}").is_in(pseudonyms))
            except polars.exceptions.InvalidOperationError:
                try:
                    pseudonyms = [int(i) for i in pseudonyms]
                except ValueError:
                    messagebox.showinfo("Warning",
                                        "Warning: invalid pseudonym!")
                revert_df = revert_df.filter(pl.col(f"Index_{columns}").is_in(pseudonyms))
            df = df.filter(pl.col(f"Index_{columns}").is_in(pseudonyms))
            if revert_df.is_empty():
                messagebox.showinfo("Warning", "Note: none of the pseudonyms mentioned were found in the mapping table!")
        pseudo = pseudPy.Pseudonymization(
            df=df,
            map_columns=columns
        )
        try:
            output = pseudo.revert_pseudonym(revert_df)
            output.write_csv(f"{output_revert}/reverted_output.csv")
            messagebox.showinfo("Success", "Revert was successful!")
        except KeyError:
            messagebox.showinfo("Failed", "Please check whether the column names match the mapping.")
    else:
        with open(input_file_revert, "r") as file:
            text = file.read()
        if pseudonyms_entry_revert.get() != "-":
            pseudonyms = pseudonyms_entry_revert.get().split(",")
            try:
                revert_df = revert_df.filter(pl.col(f"Index_{columns}").is_in(pseudonyms))
            except polars.exceptions.InvalidOperationError:
                try:
                    pseudonyms = [int(i) for i in pseudonyms]
                except ValueError:
                    messagebox.showinfo("Warning",
                                        "Warning: invalid pseudonym!")
                revert_df = revert_df.filter(pl.col(f"Index_{columns}").is_in(pseudonyms))
        if revert_df.is_empty():
            messagebox.showinfo("Warning", "Note: none of the pseudonyms mentioned were found in the mapping table!")
        pseudo = pseudPy.Pseudonymization(
            text=text,
            map_columns=columns
        )
        try:
            output = pseudo.revert_nlp_pseudonym(revert_df)
            with open(f"{output_revert}/reverted_text.txt", "w") as text_file:
                print(output, file=text_file)
            messagebox.showinfo("Success", "Revert was successful!")
        except KeyError:
            messagebox.showinfo("Failed", "Please check whether the column names match the mapping.")



def search_for_file():
    input_file = filedialog.askopenfilename()
    if input_file:
        input_file_entry.insert(0, input_file)


def search_for_dir():
    output_dir = filedialog.askdirectory()
    if output_dir:
        output_entry.insert(0, output_dir)


def clear_inputs():
    input_file_entry.delete(0, tk.END)
    output_entry.delete(0, tk.END)
    if structure_var.get() == "structured":
        filter_entry.delete(0, tk.END)
        filter_entry.insert(0, "column,operation,value,type")
        map_columns_entry.delete(0, tk.END)
        map_columns_entry.insert(0, "-")
    else:
        patterns_entry.delete(0, tk.END)
        patterns_entry.insert(0, "-")
        seed_entry.delete(0, tk.END)
        seed_entry.insert(0, "-")
        pos_type_var.set(types_of_data[0])
        all_ne_var.set(True)
    mapping_var.set(True)
    encrypt_map_var.set(False)


def modify_pattern(pattern):
    phrases = pattern.split(",")
    final_output = []
    for phrase in phrases:
        output = []
        for word in phrase.split():
            output.append({"LOWER": word.lower()})
        final_output.append(output)
    return final_output


def apply_filter():
    filter_str = filter_entry.get().split(",")
    column, op, value, type_of = filter_str[0], filter_str[1], filter_str[2], filter_str[3]
    # TODO: ignore whitespace when splitting!
    if type_of == "int":
        value = int(value)
    df = pl.read_csv(input_file_entry.get())
    if op == '>':
        filtered_df = df.filter(pl.col(column) > value)
    elif op == '<':
        filtered_df = df.filter(pl.col(column) < value)
    elif op == '==':
        filtered_df = df.filter(pl.col(column) == value)
    elif op == '!=':
        filtered_df = df.filter(pl.col(column) != value)
    else:
        raise ValueError("Invalid operation")
    filtered_df.write_csv(f'{output_entry.get()}/filtered_data.csv')
    input_file_entry.delete(0, tk.END)
    input_file_entry.insert(0, f'{output_entry.get()}/filtered_data.csv')
    messagebox.showinfo("Success", f"Filtered successfully!")


def search_for_mapping():
    input_file = filedialog.askopenfilename()
    if input_file:
        input_mapping_entry.insert(0, input_file)


def search_for_file_revert():
    input_file = filedialog.askopenfilename()
    if input_file:
        input_file_entry_revert.insert(0, input_file)


def search_for_dir_revert():
    output_dir = filedialog.askdirectory()
    if output_dir:
        output_entry_revert.insert(0, output_dir)


def clear_revert_inputs():
    input_mapping_entry.delete(0, tk.END)
    input_file_entry_revert.delete(0, tk.END)
    output_entry_revert.delete(0, tk.END)
    map_columns_entry_revert.delete(0, tk.END)
    map_columns_entry_revert.insert(0, "-")
    pseudonyms_entry_revert.delete(0, tk.END)
    pseudonyms_entry_revert.insert(0, "-")


def add_revert_widgets():

    root.geometry("700x400")

    for widget in root_frame.winfo_children():
        widget.destroy()

    root.title("Pseudonymization Tool - Revert Data")

    global input_mapping_entry, input_file_entry_revert, output_entry_revert, map_columns_entry_revert, pseudonyms_entry_revert

    input_mapping_label = tk.Label(root_frame, text="Mapping Input:")
    input_mapping_label.config(
        font=large_font
    )
    input_mapping_label.grid(row=0, column=0, pady=10, sticky=tk.W)

    input_mapping_entry = tk.Entry(root_frame, font=medium_font)
    input_mapping_entry.config(
        width=25
    )
    input_mapping_entry.grid(row=0, column=1, pady=10, sticky=tk.E)

    browse_button_map = tk.Button(
        root_frame,
        font=medium_font,
        text="Browse",
        command=search_for_mapping
    )
    browse_button_map.grid(row=0, column=2, pady=10, sticky=tk.W)

    input_file_label_revert = tk.Label(root_frame, text="Input File Path:")
    input_file_label_revert.config(
        font=large_font
    )
    input_file_label_revert.grid(row=1, column=0, pady=10, sticky=tk.W)
    input_file_entry_revert = tk.Entry(root_frame, font=medium_font)
    input_file_entry_revert.config(
        width=25
    )
    input_file_entry_revert.grid(row=1, column=1, pady=10, sticky=tk.E)

    browse_button_revert = tk.Button(
        root_frame,
        font=medium_font,
        text="Browse",
        command=search_for_file_revert
    )
    browse_button_revert.grid(row=1, column=2, pady=10, sticky=tk.W)

    output_label_revert = tk.Label(root_frame, text="Output Folder Path:")
    output_label_revert.grid(row=2, column=0, pady=10, sticky=tk.W)
    output_label_revert.config(
        font=large_font
    )
    output_entry_revert = tk.Entry(root_frame)
    output_entry_revert.config(
        width=25
    )
    output_entry_revert.grid(row=2, column=1, pady=10, sticky=tk.E)

    browse_button_revert_2 = tk.Button(
        root_frame,
        font=medium_font,
        text="Browse",
        command=search_for_dir_revert
    )
    browse_button_revert_2.grid(row=2, column=2, pady=10, sticky=tk.W)

    map_columns_label_revert = tk.Label(root_frame, text="Column Name:")
    map_columns_label_revert.config(
        font=large_font
    )
    map_columns_label_revert.grid(row=3, column=0, pady=10, sticky=tk.W)
    map_columns_entry_revert = tk.Entry(root_frame)
    map_columns_entry_revert.config(
        font=medium_font,
        width=25
    )
    map_columns_entry_revert.insert(0, "-")
    map_columns_entry_revert.grid(row=3, column=1, pady=10, sticky=tk.E)

    pseudonyms_label_revert = tk.Label(root_frame, text="Pseudonyms:")
    pseudonyms_label_revert.config(
        font=large_font
    )
    pseudonyms_label_revert.grid(row=4, column=0, pady=10, sticky=tk.W)
    pseudonyms_entry_revert = tk.Entry(root_frame)
    pseudonyms_entry_revert.config(
        font=medium_font,
        width=25
    )
    pseudonyms_entry_revert.insert(0, "-")
    pseudonyms_entry_revert.grid(row=4, column=1, pady=10, sticky=tk.E)

    reset_inputs_button = tk.Button(root_frame, text="Reset Inputs", command=clear_revert_inputs)
    reset_inputs_button.grid(row=5, column=0, pady=10, sticky=tk.W)

    confirm_revert_button = tk.Button(root_frame, text="Revert to Original", command=revert_data)
    confirm_revert_button.grid(row=5, column=1, pady=10)

    go_back_button = tk.Button(root_frame, text="Go Back", command=add_widgets)
    go_back_button.grid(row=5, column=2, pady=10)


root = tk.Tk()
root.title("Pseudonymization Tool")
root.geometry("700x700")
root.config(padx=20, pady=20)

root_frame = tk.Frame(root)
root_frame.pack(padx=20, pady=20)


def add_widgets():

    root.geometry("700x600")

    if root_frame.winfo_children():
        for widget in root_frame.winfo_children():
            widget.destroy()

    if structure_var.get() == "structured":
        root.title("Pseudonymization Tool - Structured Data")
    else:
        root.title("Pseudonymization Tool - Unstructured Data")

    global large_font, medium_font, map_method_var, input_file_entry, output_entry, filter_entry, map_columns_entry, \
        mapping_var, encrypt_map_var, all_ne_var, pos_type_var, patterns_entry, seed_entry, types_of_data

    method_options = [
        'counter',
        'encrypt',
        'decrypt',
        'random1',
        'random4',
        'hash',
        'hash-salt',
        'merkle-tree',
        'faker',
        'faker-name',
        'faker-loc',
        'faker-email',
        'faker-phone'
    ]

    large_font = tkFont.Font(family="Helvetica, Arial, sans-serif", size=16, weight="bold")
    medium_font = tkFont.Font(family="Helvetica, Arial, sans-serif", size=14)

    go_back_button = tk.Button(root_frame, text="Go Back", command=check_structure)
    go_back_button.grid(row=0, column=0, pady=10, sticky=tk.W)

    clear_button = tk.Button(root_frame, text="Reset Inputs", command=clear_inputs)
    clear_button.grid(row=0, column=1, pady=10, sticky=tk.W)

    revert_button = tk.Button(root_frame, text="Revert Data", command=add_revert_widgets)
    revert_button.grid(row=0, column=2, pady=10, sticky=tk.W)

    map_method_label = tk.Label(root_frame, text="Method:")
    map_method_label.config(
        font=large_font
    )
    map_method_label.grid(row=1, column=0, pady=10, sticky=tk.W)

    map_method_var = tk.StringVar(root_frame)
    map_method_var.set(method_options[0])
    map_method_entry = tk.OptionMenu(root_frame, map_method_var, *method_options)
    map_method_entry.config(
        font=medium_font,
        width=10
    )
    map_method_entry.grid(row=1, column=1, pady=10, sticky=tk.E)

    input_file_label = tk.Label(root_frame, text="Input File Path:")
    input_file_label.config(
        font=large_font
    )
    input_file_label.grid(row=2, column=0, pady=10, sticky=tk.W)
    input_file_entry = tk.Entry(root_frame, font=medium_font)
    input_file_entry.config(
        width=25
    )
    input_file_entry.grid(row=2, column=1, pady=10, sticky=tk.E)

    browse_button = tk.Button(
        root_frame,
        font=medium_font,
        text="Browse",
        command=search_for_file
    )
    browse_button.grid(row=2, column=2, pady=10, sticky=tk.W)

    output_label = tk.Label(root_frame, text="Output Folder Path:")
    output_label.grid(row=3, column=0, pady=10, sticky=tk.W)
    output_label.config(
        font=large_font
    )
    output_entry = tk.Entry(root_frame)
    output_entry.config(
        width=25
    )
    output_entry.grid(row=3, column=1, pady=10, sticky=tk.E)

    browse_button_2 = tk.Button(
        root_frame,
        font=medium_font,
        text="Browse",
        command=search_for_dir
    )
    browse_button_2.grid(row=3, column=2, pady=10, sticky=tk.W)

    if structure_var.get() == "structured":

        filter_label = tk.Label(root_frame, text="Filter data:", font=large_font)
        filter_label.grid(row=4, column=0, pady=10, sticky=tk.W)
        filter_entry = tk.Entry(root_frame, font=medium_font)
        filter_entry.config(
            font=medium_font,
            width=25
        )
        filter_entry.insert(0, "column,operation,value,type")
        filter_entry.grid(row=4, column=1, pady=10, sticky=tk.E)

        apply_button = tk.Button(root_frame, text="Apply Filter", command=apply_filter, font=medium_font)
        apply_button.grid(row=4, column=2, pady=10, sticky=tk.W)

        map_columns_label = tk.Label(root_frame, text="Columns:")
        map_columns_label.config(
            font=large_font
        )
        map_columns_label.grid(row=5, column=0, pady=10, sticky=tk.W)
        map_columns_entry = tk.Entry(root_frame)
        map_columns_entry.config(
            font=medium_font,
            width=25
        )
        map_columns_entry.insert(0, "-")
        map_columns_entry.grid(row=5, column=1, pady=10, sticky=tk.E)

    mapping_label = tk.Label(root_frame, text="Include Mapping?")
    mapping_label.grid(row=6, column=0, pady=10, sticky=tk.W)
    mapping_label.config(
        font=large_font
    )
    mapping_var = tk.BooleanVar(root_frame)
    mapping_var.set(True)
    mapping_entry = tk.Checkbutton(root_frame, variable=mapping_var)
    mapping_entry.config(
        font=medium_font,
        width=10
    )
    mapping_entry.grid(row=6, column=1, pady=10, sticky=tk.E)

    encrypt_map_label = tk.Label(root_frame, text="Encrypt Mapping?")
    encrypt_map_label.grid(row=7, column=0, pady=10, sticky=tk.W)
    encrypt_map_label.config(
        font=large_font
    )
    encrypt_map_var = tk.BooleanVar(root_frame)
    encrypt_map_var.set(False)
    encrypt_map_entry = tk.Checkbutton(root_frame, variable=encrypt_map_var)
    encrypt_map_entry.config(
        font=medium_font,
        width=10
    )
    encrypt_map_entry.grid(row=7, column=1, pady=10, sticky=tk.E)

    if structure_var.get() == "free text":
        all_ne_label = tk.Label(root_frame, text="All Named Entities Only?")
        all_ne_label.grid(row=8, column=0, pady=10, sticky=tk.W)
        all_ne_label.config(
            font=large_font
        )
        all_ne_var = tk.BooleanVar(root_frame)
        all_ne_var.set(True)
        all_ne_entry = tk.Checkbutton(root_frame, variable=all_ne_var)
        all_ne_entry.config(
            font=medium_font,
            width=10
        )
        all_ne_entry.grid(row=8, column=1, pady=10, sticky=tk.E)

        types_of_data = [
            "-",
            "Names",
            "Locations",
            "Organizations",
            "Emails",
            "Phone-Numbers",
            "Others"
        ]

        pos_type_label = tk.Label(root_frame, text="Type of Data:")
        pos_type_label.grid(row=9, column=0, pady=10, sticky=tk.W)
        pos_type_label.config(
            font=large_font
        )
        pos_type_var = tk.StringVar(root_frame)
        pos_type_var.set(types_of_data[0])
        pos_type_entry = tk.OptionMenu(root_frame, pos_type_var, *types_of_data)
        pos_type_entry.config(
            font=medium_font,
            width=10
        )
        pos_type_entry.grid(row=9, column=1, pady=10, sticky=tk.E)

        patterns_label = tk.Label(root_frame, text="Word Combinations:")
        patterns_label.grid(row=10, column=0, pady=10, sticky=tk.W)
        patterns_label.config(
            font=large_font
        )
        patterns_entry = tk.Entry(root_frame)
        patterns_entry.config(
            width=25
        )
        patterns_entry.insert(0, "-")
        patterns_entry.grid(row=10, column=1, pady=10, sticky=tk.E)

    seed_label = tk.Label(root_frame, text="Seed:")
    seed_label.grid(row=11, column=0, pady=10, sticky=tk.W)
    seed_label.config(
        font=large_font
    )
    seed_entry = tk.Entry(root_frame)
    seed_entry.config(
        width=25
    )
    seed_entry.insert(0, "-")
    seed_entry.config(
        font=medium_font
    )
    seed_entry.grid(row=11, column=1, pady=10, sticky=tk.E)

    register_button = tk.Button(root_frame, text="Go!", command=initialize_pseudonym)
    register_button.grid(row=12, column=1, pady=10)


def check_structure():

    root.geometry("700x400")

    if root_frame.winfo_children():
        for widget in root_frame.winfo_children():
            widget.destroy()

    root.title("Pseudonymization Tool")

    global structure_var

    welcome_label = tk.Label(root_frame, text="Welcome to Pseudonymization Tool!")
    welcome_label.config(
        font=tkFont.Font(family="Helvetica, Arial, sans-serif", size=25, weight="bold")
    )
    welcome_label.grid(row=0, column=0, columnspan=3, pady=30, sticky=tk.N)

    form_options = [
        "structured",
        "free text"
    ]

    structure_label = tk.Label(root_frame, text="Form of Data:")
    structure_label.config(
        font=tkFont.Font(family="Helvetica, Arial, sans-serif", size=16, weight="bold")
    )
    structure_label.grid(row=1, column=0, pady=10, sticky=tk.E)

    structure_var = tk.StringVar(root_frame)
    structure_var.set(form_options[0])
    structure_entry = tk.OptionMenu(root_frame, structure_var, *form_options)
    structure_entry.grid(row=1, column=1, pady=10, sticky=tk.E)

    next_button = tk.Button(root_frame, text="Next", command=add_widgets)
    next_button.grid(row=2, column=2, pady=10, sticky=tk.E)


check_structure()

root.mainloop()
