"""`python -m pipeline.deliver.answers.model [--check]`: write (or check) the app's generated copy of the schema."""
import sys

from pipeline.deliver.answers.model.emit import main

if __name__ == "__main__":
    sys.exit(main())
