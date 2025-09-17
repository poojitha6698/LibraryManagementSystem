import os
from supabase import create_client, Client   # pip install supabase
from dotenv import load_dotenv               # pip install python-dotenv
from tabulate import tabulate                 # pip install tabulate

# Load environment variables
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
sb: Client = create_client(url, key)

# -----------------------
# Create / Insert helpers
# -----------------------
def add_member(name, email):
    resp = sb.table("members").insert({"name": name, "email": email}).execute()
    return resp.data

def add_book(title, author, category, stock=1):
    resp = sb.table("books").insert({"title": title, "author": author, "category": category, "stock": stock}).execute()
    return resp.data

# -----------------------
# Read / Query functions
# -----------------------
def list_books():
    resp = sb.table("books").select("*").execute()
    rows = resp.data
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="psql"))
    else:
        print("(no books)")

def search_books(term, field="title"):
    resp = sb.table("books").select("*").ilike(field, f"%{term}%").execute()
    rows = resp.data
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="psql"))
    else:
        print("(no matches)")

def show_member(member_id):

    member = sb.table("members").select("*").eq("member_id", member_id).execute().data
    borrows = sb.table("borrow_records").select("record_id, book_id, borrow_date, return_date").eq("member_id", member_id).execute().data
    print("Member:")
    print(tabulate(member, headers="keys", tablefmt="psql") if member else "Not found")
    print("\nBorrow history:")
    print(tabulate(borrows, headers="keys", tablefmt="psql") if borrows else "(no borrow history)")

# -----------------------
# Update functions
# -----------------------
def update_book_stock(book_id, new_stock):
    resp = sb.table("books").update({"stock": new_stock}).eq("book_id", book_id).execute()
    return resp.data

def update_member_email(member_id, new_email):
    resp = sb.table("members").update({"email": new_email}).eq("member_id", member_id).execute()
    return resp.data

# -----------------------
# Delete functions
# -----------------------
def delete_member(member_id):
    open_borrows = sb.table("borrow_records").select("*").eq("member_id", member_id).is_("return_date", None).execute().data
    if open_borrows:
        print("Cannot delete member: they have unreturned books.")
        return
    sb.table("members").delete().eq("member_id", member_id).execute()
    print(f"Member {member_id} deleted.")

def delete_book(book_id):
    open_borrows = sb.table("borrow_records").select("*").eq("book_id", book_id).is_("return_date", None).execute().data
    if open_borrows:
        print("Cannot delete book: it is currently borrowed.")
        return
    sb.table("books").delete().eq("book_id", book_id).execute()
    print(f"Book {book_id} deleted.")

# -----------------------
# Borrow / Return
# -----------------------
def borrow_book(member_id, book_id):
    book = sb.table("books").select("stock").eq("book_id", book_id).execute().data
    if not book:
        print("Book not found.")
        return
    if book[0]["stock"] <= 0:
        print("Book not available.")
        return
    sb.table("books").update({"stock": book[0]["stock"] - 1}).eq("book_id", book_id).execute()
    sb.table("borrow_records").insert({"member_id": member_id, "book_id": book_id}).execute()
    print(f"Borrow successful: member {member_id} → book {book_id}")

def return_book(member_id, book_id):
    borrow = sb.table("borrow_records").select("*").eq("member_id", member_id).eq("book_id", book_id).is_("return_date", None).limit(1).execute().data
    if not borrow:
        print("No active borrow found.")
        return
    rec_id = borrow[0]["record_id"]
    sb.table("borrow_records").update({"return_date": "now()"}).eq("record_id", rec_id).execute()
    book = sb.table("books").select("stock").eq("book_id", book_id).execute().data[0]
    sb.table("books").update({"stock": book["stock"] + 1}).eq("book_id", book_id).execute()
    print(f"Return successful: record {rec_id}")

# -----------------------
# Reports
# -----------------------
def report_top_borrowed(limit=5):
    resp = (
        sb.table("borrow_records")
        .select("book_id, books(title, author)")
        .execute()
    )
    rows = resp.data

    # Count borrows per book
    counts = {}
    for r in rows:
        b = r["books"]
        key = (r["book_id"], b["title"], b["author"])
        counts[key] = counts.get(key, 0) + 1

    # Sort and limit
    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]

    # Print nicely
    print("+---------+-----------------+-----------------+---------------+")
    print("| book_id | title           | author          | borrow_count  |")
    print("+---------+-----------------+-----------------+---------------+")
    for (book_id, title, author), count in sorted_counts:
        print(f"| {book_id:<7} | {title:<15} | {author:<15} | {count:<13} |")
    print("+---------+-----------------+-----------------+---------------+")


def report_overdue_members():
    sql = """
    SELECT members.member_id, members.name, members.email, books.title, borrow_records.borrow_date
    FROM borrow_records
    JOIN members ON members.member_id = borrow_records.member_id
    JOIN books ON books.book_id = borrow_records.book_id
    WHERE borrow_records.return_date IS NULL
    AND borrow_records.borrow_date < NOW() - INTERVAL '14 days';
    """
    rows = sb.rpc("exec_sql", {"sql": sql}).execute().data
    print(tabulate(rows, headers="keys", tablefmt="psql") if rows else "(no overdue)")

def report_borrows_per_member():
    sql = """
    SELECT members.member_id, members.name, COUNT(borrow_records.record_id) AS total_borrows
    FROM borrow_records
    JOIN members ON members.member_id = borrow_records.member_id
    GROUP BY members.member_id, members.name
    ORDER BY total_borrows DESC;
    """
    rows = sb.rpc("exec_sql", {"sql": sql}).execute().data
    print(tabulate(rows, headers="keys", tablefmt="psql") if rows else "(no borrows)")

# -----------------------
# CLI Menu
# -----------------------
def menu():
    while True:
        choice = input("""
Library CLI:
1) Register member
2) Add book
3) List books
4) Search books
5) Show member details
6) Update book stock
7) Update member email
8) Delete member
9) Delete book
10) Borrow book
11) Return book
12) Report: top borrowed books
13) Report: overdue members
14) Report: total borrows per member
0) Quit
> """).strip()

        if choice == "1":
            add_member(input("Name: "), input("Email: "))
        elif choice == "2":
            add_book(input("Title: "), input("Author: "), input("Category: "), int(input("Stock: ") or "1"))
        elif choice == "3":
            list_books()
        elif choice == "4":
            search_books(input("Search term: "), input("Field (title|author|category): ") or "title")
        elif choice == "5":
            show_member(int(input("Member ID: ")))
        elif choice == "6":
            update_book_stock(int(input("Book ID: ")), int(input("New stock: ")))
        elif choice == "7":
            update_member_email(int(input("Member ID: ")), input("New email: "))
        elif choice == "8":
            delete_member(int(input("Member ID: ")))
        elif choice == "9":
            delete_book(int(input("Book ID: ")))
        elif choice == "10":
            borrow_book(int(input("Member ID: ")), int(input("Book ID: ")))
        elif choice == "11":
            return_book(int(input("Member ID: ")), int(input("Book ID: ")))
        elif choice == "12":
            report_top_borrowed()
        elif choice == "13":
            report_overdue_members()
        elif choice == "14":
            report_borrows_per_member()
        elif choice == "0":
            print("Goodbye!")
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    menu()
