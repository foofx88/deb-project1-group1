from weatherapi.etl.extract_cities import extract_cities

extracted_df = extract_cities()

citynames = extracted_df['city'].tolist()

def test_extract_cities(citynames):
    result = "passed"
    try:
        #assert performing name checks
        for cityname in citynames:
            if (" " in cityname) or ("-" in cityname):
                name_check = "failed"
            else:
                name_check="passed"
        #assert
        assert name_check == result, f"There is an error in the Cities extraction process. Identified - {cityname}" #if fail
        print(f"Cities extraction passed - {citynames}") #if pass

    except AssertionError as msg:
        print(msg)



if __name__ == "__main__": 
    # run the test
    test_extract_cities(citynames)