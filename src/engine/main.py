#!/usr/bin/env python3
from engine import Engine, Ingestor, Aggregator, Preparator, Analytics
import sys

def main():
    try:
        command = sys.argv[1]
        if command=='ingest':
            engine = Ingestor.Ingestor()
            engine.start()
        elif command=='aggregate':
            engine = Aggregator.Aggregator()
            engine.start()
        elif command=='transform':
            engine = Preparator.Preparator()
            engine.start()
        elif command=='analyze':
            engine = Analytics.Analytics()
            engine.start()
        else:
            raise ValueError
    except KeyboardInterrupt:
        print("Turning Off The Engine...")
    except:
        Engine.wrong_input(sys.exc_info())
        None

if __name__ == "__main__":
    sys.exit(main())