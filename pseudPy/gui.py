import os
import re
import tkinter as tk
import polars as pl
from tkinter import messagebox
from tkinter import font as tkFont
import pandas as pd

import Pseudonymization as pseudPy
import polars.exceptions
from tkinter import filedialog


def initialize_pseudonym():
    """Getting user input and completing pseudonymization"""
    map_method = map_method_var.get()
    input_file = input_file_entry.get()
    if not os.path.exists(input_file):
        messagebox.showinfo("Failed", "Input path does not exist!")
    patterns = patterns_entry.get()
    if patterns == "-" or patterns == "column,operation,value,type":
        patterns = None
    else:
        if structure_var.get() == "free text":
            patterns = modify_pattern(patterns)
        else:
            patterns = patterns.split(",")
            if patterns[3].strip() == "int":
                patterns[2] = int(patterns[2].strip())
    output = output_entry.get()
    if not os.path.exists(output):
        messagebox.showinfo("Failed", "Output path does not exist!")
    mapping = mapping_var.get()
    encrypt_map = encrypt_map_var.get()
    seed = seed_entry.get()
    if seed == "-":
        seed = None
    else:
        seed = int(seed)

    if structure_var.get() == "structured":
        map_columns = map_columns_entry.get()
        if map_columns == "-":
            map_columns = None
        else:
            map_columns = map_columns.split(",")
            map_columns = [i.strip() for i in map_columns]

        pseudo = pseudPy.Pseudonymization(
            map_columns=map_columns,
            map_method=map_method,
            input_file=input_file,
            output=output,
            mapping=mapping,
            encrypt_map=encrypt_map,
            seed=seed,
            patterns=patterns
        )

        pseudo.pseudonym()
        messagebox.showinfo("Success", "Pseudonymization successful!")
    elif structure_var.get() == "free text":
        pos_type_selected = pos_type_list.curselection()
        pos_type = [pos_type_list.get(i) for i in pos_type_selected]
        if not pos_type:
            pos_type = None
        all_ne = all_ne_var.get()

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
    """Revert the data to its original state"""
    input_mapping = input_mapping_entry.get()
    if not os.path.exists(input_mapping):
        messagebox.showinfo("Failed", "Mapping path does not exist!")
    input_file_revert = input_file_entry_revert.get()
    if not os.path.exists(input_file_revert):
        messagebox.showinfo("Failed", "Input path does not exist!")
    output_revert = output_entry_revert.get()
    if not os.path.exists(output_revert):
        messagebox.showinfo("Failed", "Output path does not exist!")

    revert_df = pl.read_csv(input_mapping)
    columns = map_columns_entry_revert.get()
    if not columns or (columns == "-"):
        messagebox.showinfo("Failed", "Please specify the column to map!")
    if structure_var.get() == "structured":
        df = pl.read_csv(input_file_revert)
        if pseudonyms_entry_revert.get("1.0", "end-1c") != "-":
            pseudonyms = pseudonyms_entry_revert.get("1.0", "end-1c").split(",")
            pseudonyms = [i.strip() for i in pseudonyms]
        else:
            pseudonyms = None
        pseudo = pseudPy.Pseudonymization(
            df=df,
            map_columns=columns
        )
        try:
            if pseudonyms is not None:
                output = pseudo.revert_pseudonym(revert_df, pseudonyms)
            else:
                output = pseudo.revert_pseudonym(revert_df)
            output.write_csv(f"{output_revert}/reverted_output.csv")
            messagebox.showinfo("Success", "Revert was successful!")
        except KeyError:
            messagebox.showinfo("Failed", "Please check whether the column names match the mapping.")
    elif structure_var.get() == "free text":
        with open(input_file_revert, "r") as file:
            text = file.read()
        if pseudonyms_entry_revert.get("1.0", "end-1c") != "-":
            pseudonyms = pseudonyms_entry_revert.get("1.0", "end-1c").split(",")
            pseudonyms = [i.strip() for i in pseudonyms]
        else:
            pseudonyms = None
        if len(revert_df.columns) == 1:
            try:
                revert_df = revert_df.filter(pl.col(f"Index_{columns}").is_in(pseudonyms))
            except polars.exceptions.InvalidOperationError:
                pseudonyms = [int(i) for i in pseudonyms]
                revert_df = revert_df.filter(pl.col(f"Index_{columns}").is_in(pseudonyms))
            mapping = pseudPy.Mapping(revert_df, first_tier=columns, output=output_revert)
            text = mapping.decrypt_nlp_tier(text)
            with open(f'{output_revert}/text.txt', 'w') as file:
                print(text, file=file)
                messagebox.showinfo("Success", "Revert was successful!")
        else:
            pseudo = pseudPy.Pseudonymization(
                text=text,
                map_columns=columns
            )
            try:
                if pseudonyms is not None:
                    output = pseudo.revert_nlp_pseudonym(revert_df, pseudonyms)
                else:
                    output = pseudo.revert_nlp_pseudonym(revert_df)
                with open(f"{output_revert}/reverted_text.txt", "w") as text_file:
                    print(output, file=text_file)
                messagebox.showinfo("Success", "Revert was successful!")
            except KeyError:
                messagebox.showinfo("Failed", "Please check whether the column names match the mapping.")


def search_for_file():
    """Browsing the input file on the PC"""
    input_file = filedialog.askopenfilename()
    if input_file:
        input_file_entry.insert(0, input_file)


def search_for_dir():
    """Browsing the output directory on the PC"""
    output_dir = filedialog.askdirectory()
    if output_dir:
        output_entry.insert(0, output_dir)


def clear_inputs():
    """Reset the entry widgets"""
    input_file_entry.delete(0, tk.END)
    output_entry.delete(0, tk.END)
    if structure_var.get() == "structured":
        patterns_entry.delete(0, tk.END)
        patterns_entry.insert(0, "column,operation,value,type")
        map_columns_entry.delete(0, tk.END)
        map_columns_entry.insert(0, "-")
        mapping_var.set(True)
        encrypt_map_var.set(False)
    elif structure_var.get() == "free text":
        patterns_entry.delete(0, tk.END)
        patterns_entry.insert(0, "-")
        seed_entry.delete(0, tk.END)
        seed_entry.insert(0, "-")
        pos_type_list.selection_clear(0, tk.END)
        all_ne_var.set(False)
        mapping_var.set(True)
        encrypt_map_var.set(False)
    elif structure_var.get() == "k-anonymity":
        k_entry.delete(0, tk.END)
        k_entry.insert(0, '0')
        mask_str_var.set(True)
        agg_entry.delete(0, tk.END)
        agg_entry.insert(0, '-')
        gap_entry.delete(0, tk.END)
        gap_entry.insert(0, '-')


def modify_pattern(pattern):
    """Modify the spaCy pattern for the rule-based matching"""
    phrases = pattern.split(",")
    final_output = []
    for phrase in phrases:
        output = []
        for word in phrase.split():
            output.append({"LOWER": word.lower()})
        final_output.append(output)
    return final_output


def search_for_mapping():
    """Browsing the mapping file on the PC"""
    input_file = filedialog.askopenfilename()
    if input_file:
        input_mapping_entry.insert(0, input_file)


def search_for_file_revert():
    """Browsing the file for revert on the PC"""
    input_file = filedialog.askopenfilename()
    if input_file:
        input_file_entry_revert.insert(0, input_file)


def search_for_dir_revert():
    """Browsing the output directory for revert on the PC"""
    output_dir = filedialog.askdirectory()
    if output_dir:
        output_entry_revert.insert(0, output_dir)


def clear_revert_inputs():
    """Resetting the input widgets in the revert window"""
    input_mapping_entry.delete(0, tk.END)
    input_file_entry_revert.delete(0, tk.END)
    output_entry_revert.delete(0, tk.END)
    map_columns_entry_revert.delete(0, tk.END)
    map_columns_entry_revert.insert(0, "-")
    pseudonyms_entry_revert.delete("1.0", tk.END)
    pseudonyms_entry_revert.insert("1.0", "-")


def add_revert_widgets():
    """Add revert widgets to the revert data window"""
    root.geometry("700x500")

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
    pseudonyms_entry_revert = tk.Text(root_frame, width=25, height=4)
    pseudonyms_entry_revert.config(
        font=medium_font,
        width=25
    )
    pseudonyms_entry_revert.insert("1.0", "-")
    pseudonyms_entry_revert.grid(row=4, column=1, pady=10, sticky=tk.E)

    reset_inputs_button = tk.Button(root_frame, text="Reset Inputs", command=clear_revert_inputs)
    reset_inputs_button.grid(row=5, column=0, pady=10, sticky=tk.W)

    confirm_revert_button = tk.Button(root_frame, text="Revert to Original", command=revert_data)
    confirm_revert_button.grid(row=5, column=1, pady=10)

    go_back_button = tk.Button(root_frame, text="Go Back", command=add_widgets)
    go_back_button.grid(row=5, column=2, pady=10)


# initialize the tkinter root window
root = tk.Tk()
root.title("Pseudonymization Tool")
root.geometry("700x700")
root.config(padx=20, pady=20)

root_frame = tk.Frame(root)
root_frame.pack(padx=20, pady=20)


def update_types_of_data(*args):
    """Select all named entities in the 'Type of Data' if the 'All Named Entities Only?' set to True"""
    selection = all_ne_var.get()
    pos_type_list.selection_clear(0, tk.END)
    if selection:
        indices_to_select = [0, 1, 2]  # Indices of "Names", "Locations", "Organizations"
        for index in indices_to_select:
            pos_type_list.selection_set(index)


def add_widgets():
    """Add widgets to the structured or free text or k-anonymization data windows"""
    if structure_var.get() == "free text":
        root.geometry("700x750")
    else:
        root.geometry("750x700")

    if root_frame.winfo_children():
        for widget in root_frame.winfo_children():
            widget.destroy()

    if structure_var.get() == "structured":
        root.title("Pseudonymization Tool - Structured Data")
    elif structure_var.get() == "free text":
        root.title("Pseudonymization Tool - Unstructured Data")
    elif structure_var.get() == "k-anonymity":
        root.title("Pseudonymization Tool - k-Anonymity")

    global large_font, medium_font, map_method_var, input_file_entry, output_entry, map_columns_entry, \
        mapping_var, encrypt_map_var, all_ne_var, patterns_entry, seed_entry, types_of_data, k_entry, \
        mask_str_var, agg_entry, gap_entry, pos_type_list, types_of_data

    if structure_var.get() == "free text":
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
            'faker-phone',
            'faker-org'
        ]
    elif structure_var.get() == "structured":
        method_options = [
            'counter',
            'encrypt',
            'decrypt',
            'random1',
            'random4',
            'hash',
            'hash-salt',
            'merkle-tree',
            'faker-name',
            'faker-loc',
            'faker-email',
            'faker-phone',
            'faker-org'
        ]

    large_font = tkFont.Font(family="Helvetica, Arial, sans-serif", size=16, weight="bold")
    medium_font = tkFont.Font(family="Helvetica, Arial, sans-serif", size=14)

    go_back_button = tk.Button(root_frame, text="Go Back", command=check_structure)
    go_back_button.grid(row=0, column=0, pady=10, sticky=tk.W)

    clear_button = tk.Button(root_frame, text="Reset Inputs", command=clear_inputs)
    clear_button.grid(row=0, column=1, pady=10, sticky=tk.W)

    revert_button = tk.Button(root_frame, text="Revert Data", command=add_revert_widgets)
    revert_button.grid(row=0, column=2, pady=10, sticky=tk.W)

    if structure_var.get() == "free text" or structure_var.get() == "structured":

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

    if structure_var.get() == "k-anonymity":
        k_label = tk.Label(root_frame, text="k:", font=large_font)
        k_label.grid(row=4, column=0, pady=10, sticky=tk.W)
        k_entry = tk.Entry(root_frame, font=medium_font)
        k_entry.config(
            font=medium_font,
            width=25
        )
        k_entry.insert(0, "0")
        k_entry.grid(row=4, column=1, pady=10, sticky=tk.E)

        mask_str_label = tk.Label(root_frame, text="Mask all textual data?")
        mask_str_label.grid(row=5, column=0, pady=10, sticky=tk.W)
        mask_str_label.config(
            font=large_font
        )
        mask_str_var = tk.BooleanVar(root_frame)
        mask_str_var.set(True)
        mask_str_entry = tk.Checkbutton(root_frame, variable=mask_str_var)
        mask_str_entry.config(
            font=medium_font,
            width=10
        )
        mask_str_entry.grid(row=5, column=1, pady=10, sticky=tk.E)

        add_label = tk.Label(root_frame, text="Additional")
        add_label.config(
            font=tkFont.Font(family="Helvetica, Arial, sans-serif", size=20, weight="bold"),
            fg='#fca311'
        )
        add_label.grid(row=6, column=0, pady=30, sticky=tk.W)

        agg_label = tk.Label(root_frame, text="Aggregate columns:", font=large_font)
        agg_label.grid(row=7, column=0, pady=10, sticky=tk.W)
        agg_entry = tk.Entry(root_frame, font=medium_font)
        agg_entry.config(
            font=medium_font,
            width=25
        )
        agg_entry.insert(0, "-")
        agg_entry.grid(row=7, column=1, pady=10, sticky=tk.E)

        gap_label = tk.Label(root_frame, text="Gap:", font=large_font)
        gap_label.grid(row=8, column=0, pady=10, sticky=tk.W)
        gap_entry = tk.Entry(root_frame, font=medium_font)
        gap_entry.config(
            font=medium_font,
            width=25
        )
        gap_entry.insert(0, "-")
        gap_entry.grid(row=8, column=1, pady=10, sticky=tk.E)


    if structure_var.get() == "structured":

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

    if structure_var.get() == "free text" or structure_var.get() == "structured":

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
        all_ne_var.set(False)
        all_ne_entry = tk.Checkbutton(root_frame, variable=all_ne_var)
        all_ne_entry.config(
            font=medium_font,
            width=10
        )
        all_ne_entry.grid(row=8, column=1, pady=10, sticky=tk.E)
        all_ne_var.trace('w', update_types_of_data)

        types_of_data = [
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

        pos_type_list = tk.Listbox(root_frame, selectmode=tk.MULTIPLE, height=6)
        pos_type_list.grid(row=9, column=1, padx=10, pady=10, sticky=tk.E)

        for option in types_of_data:
            pos_type_list.insert(tk.END, option)

    if structure_var.get() == "free text" or structure_var.get() == "structured":

        patterns_label = tk.Label(root_frame, text="Filter for:")
        patterns_label.grid(row=10, column=0, pady=10, sticky=tk.W)
        patterns_label.config(
            font=large_font
        )
        patterns_entry = tk.Entry(root_frame)
        patterns_entry.config(
            width=25
        )
        patterns_entry.grid(row=10, column=1, pady=10, sticky=tk.E)

        if structure_var.get() == "structured":
            patterns_entry.insert(0, "column,operation,value,type")
        else:
            patterns_entry.insert(0, "-")

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

    if structure_var.get() == "free text" or structure_var.get() == "structured":
        register_button = tk.Button(root_frame, text="Go!", command=initialize_pseudonym)
        register_button.grid(row=12, column=1, pady=10)
    else:
        register_button = tk.Button(root_frame, text="Go!", command=k_anonym)
        register_button.grid(row=12, column=1, pady=10)

        check_k_anonymity = tk.Button(root_frame, text="Check k-anonymity", command=check_k_anon)
        check_k_anonymity.grid(row=12, column=2, pady=10)

    if structure_var.get() == "k-anonymity":
        note_label = tk.Label(root_frame, text="Note: for structured data only!", font=medium_font,
                              fg='#c1121f')
        note_label.grid(row=13, column=0, pady=10, sticky=tk.W)

        rec_label = tk.Label(root_frame, text="Use aggregation and "
                                              "k-anonymization", font=medium_font,
                             fg='#c1121f')
        rec_label.grid(row=14, column=0, pady=10, sticky=tk.W)

        rec_label_2 = tk.Label(root_frame, text="separately to avoid errors.", font=medium_font,
                             fg='#c1121f')
        rec_label_2.grid(row=15, column=0, pady=10, sticky=tk.W)


def check_structure():
    """Add widgets to the homepage, select the type of data or the type of operation to proceed"""
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
        "free text",
    ]

    structure_label = tk.Label(root_frame, text="Select Form of Data:")
    structure_label.config(
        font=tkFont.Font(family="Helvetica, Arial, sans-serif", size=16, weight="bold")
    )
    structure_label.grid(row=1, column=0, pady=10, sticky=tk.E)

    structure_var = tk.StringVar(root_frame)
    structure_var.set(form_options[0])
    structure_entry = tk.OptionMenu(root_frame, structure_var, *form_options)
    structure_entry.grid(row=1, column=1, pady=10, sticky=tk.E)

    next_button = tk.Button(root_frame, text="Next", command=add_widgets)
    next_button.grid(row=1, column=2, pady=10, sticky=tk.E)

    or_label = tk.Label(root_frame, text="or")
    or_label.config(
        font=tkFont.Font(family="Helvetica, Arial, sans-serif", size=16, weight="bold")
    )
    or_label.grid(row=2, column=0, pady=10, sticky=tk.E)

    k_anon_label = tk.Label(root_frame, text="k-Anonymize")
    k_anon_label.config(
        font=tkFont.Font(family="Helvetica, Arial, sans-serif", size=16, weight="bold")
    )
    k_anon_label.grid(row=3, column=0, pady=10, sticky=tk.E)

    arrow_label = tk.Label(root_frame, text="/   Aggregate Data:")
    arrow_label.config(
        font=tkFont.Font(family="Helvetica, Arial, sans-serif", size=16, weight="bold")
    )
    arrow_label.grid(row=3, column=1, pady=10)

    k_button = tk.Button(root_frame, text="Here", command=set_entry_and_add_widgets)
    k_button.grid(row=3, column=2, pady=10, sticky=tk.E)


def set_entry_and_add_widgets():
    structure_var.set('k-anonymity')
    add_widgets()


def k_anonym():
    """K-anonymize data"""
    def is_valid_date(date):
        date_regex = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        return bool(date_regex.match(date))

    input_file = input_file_entry.get()
    if not os.path.exists(input_file):
        messagebox.showinfo("Failed", "Output path does not exist!")
    output = output_entry.get()
    if not os.path.exists(output):
        messagebox.showinfo("Failed", "Output path does not exist!")

    input_df = pd.read_csv(input_file)

    mask_str_bool = mask_str_var.get()
    k = int(k_entry.get())
    x = gap_entry.get()
    x = None if x == '-' else int(x)

    agg_columns = agg_entry.get()
    if agg_columns == "-":
        agg_columns = None
    else:
        agg_columns_list = agg_columns.split(",")
        agg_columns_list = [i.strip() for i in agg_columns_list]

        if x is None:
            messagebox.showinfo("Failed", "Please enter the GAP entry of type Integer!")
        for col in agg_columns_list:
            if pd.api.types.is_float_dtype(input_df[col]) or pd.api.types.is_integer_dtype(input_df[col]):
                agg = pseudPy.Aggregation(
                    column=col,
                    method=['number', x],
                    df=input_df
                )
                input_df = agg.group()
            elif is_valid_date(input_df[col].iloc[0]):
                agg = pseudPy.Aggregation(
                    column=col,
                    method=['dates-to-years', x],
                    df=input_df
                )
                input_df = agg.group_dates_to_years()

    df_header = input_df.columns.to_list()

    depths = {}
    if k > 0:
        for col in df_header:
            depths[col] = k-1
        k_anonymity = pseudPy.KAnonymity(df=input_df, depths=depths, k=k, mask_others=mask_str_bool)
        grouped = k_anonymity.k_anonymity()
        grouped.to_csv(f"{output}/k-anonym-output.csv", index=False)
        messagebox.showinfo("Success", f"{k}-anonymization was successful!")
    else:
        input_df.to_csv(f"{output}/aggregated_output.csv", index=False)
        messagebox.showinfo("Success", "Aggregation was successful!")


def check_k_anon():
    """Check if the data is k-anonymous"""
    input_file = input_file_entry.get()
    if not os.path.exists(input_file):
        messagebox.showinfo("Failed", "Output path does not exist!")

    input_df = pd.read_csv(input_file)
    k = int(k_entry.get())
    k_anonymity = pseudPy.KAnonymity(df=input_df,k=k)
    is_k_anon = k_anonymity.is_k_anonymized()

    if is_k_anon:
        messagebox.showinfo("Success", f"The data is {k}-anonymous!")
    else:
        messagebox.showinfo("Failed", f"The data is not {k}-anonymous!")


check_structure()

root.mainloop()
