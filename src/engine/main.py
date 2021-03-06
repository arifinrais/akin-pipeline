#!/usr/bin/env python3
from engine import Engine, Ingestor, Aggregator, Preparator, Analytics, RQPreparator#, Scraper
from engine.EngineHelper import WrongInputHandler
import sys, logging

def main():
    try:
        command = sys.argv[1]
        logging.basicConfig(filename='%s.log' % command, encoding='utf-8', level=logging.WARNING) # for production WARNING
        if command=='scrape':
            engine = Ingestor.Ingestor()
            engine.scrape()
        elif command=='ingest':
            engine = Ingestor.Ingestor()
            engine.start()
        elif command=='aggregate':
            engine = Aggregator.Aggregator()
            engine.start()
        elif command=='prepare':
            engine = RQPreparator.RQPreparator()
            engine.prepare()
        elif command=='transform':
            engine = RQPreparator.RQPreparator()
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