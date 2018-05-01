from pymarc import MARCReader

with open('/newvolume/marc/055000.mrc', 'rb') as fh:
    reader = MARCReader(fh)
    for record in reader:
        print(record['100']['a'])
        print(record['505'])
        # print(record['520']['a'])
        # print(record['600']['a'])
        # print(record['650']['a'])
        # print(record['651']['a'])
        print("title: " + record.title())
        print("author: " + record.author())
        isbn = record.isbn() or ""
        print("isbn: " + isbn)
        subjects = record.subjects() or ""
        # print("subjects: " + subjects)
        # print("location: " + record.location())
        # print("notes: " + record.notes())
        # print("physical description: " + record.physicaldescription())
        print("publisher: " + record.publisher())
        print("pubyear: " + record.pubyear())
