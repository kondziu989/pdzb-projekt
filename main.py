from fetch_files import fetch_files
import compare_files


compare_files.make_dirs()

done = False
i = 0

while not done and i < 3:
    i += 1
    print('Iteration ', i)
    try:
        fetch_files()
        done = True
    except KeyboardInterrupt:
        print('Interrupted...')
    except Exception as e:
        print('There was an error:', e)

if not done:
    raise Exception('Could not download kaggle dataset.')

compare_files.run_comparison()
