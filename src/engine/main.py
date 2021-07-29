#!/usr/bin/env python3
from engine import Engine, Ingestor, Aggregator, Preparator, Analytics#, Scraper
from engine.EngineHelper import WrongInputHandler
import sys

def main():
    try:
        command = sys.argv[1]
        if command=='scrape':
            engine = Ingestor.Ingestor()
            engine.scrape()
        elif command=='ingest':
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
        WrongInputHandler(sys.exc_info())

if __name__ == "__main__":
    sys.exit(main())