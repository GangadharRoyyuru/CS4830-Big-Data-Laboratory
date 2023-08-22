def print_num_lines_infile(data,context):

        from google.cloud import storage
        file = data['name']

        client  = storage.Client()
        bucket  = client.get_bucket('me19b190-assignment1-cs4830')
        blob    = bucket.get_blob(file)
        x       = blob.download_as_string()
        x       = x.decode('utf-8')

        #code  to count the number of lines in a file and print the file name
        print("Name of the file: ", file)
        print("Number of lines: ", len(x.split("\n")))



