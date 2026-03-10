import pandas as pd
import numpy as np
import random
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def apply_chaos(df: pd.DataFrame, chaos_probability: float = 0.3) -> tuple[pd.DataFrame, list]:
    """
    Randomly introduce real world data quality problems.
    Returns corrupted dataframe and list of chaos events applied.
    """
    
    if random.random() > chaos_probability:
        logger.info("No chaos applied this run")
        return df, []
    
    df = df.copy()
    chaos_events = []
    
    chaos_types = [
        "missing_values",
        "duplicate_records", 
        "schema_drift",
        "late_data",
        "corrupted_values",
        "missing_series"
    ]
    
    # Pick 1-2 chaos types randomly
    selected = random.sample(chaos_types, k=random.randint(1, 2))
    
    for chaos_type in selected:
        
        if chaos_type == "missing_values":
            # Randomly null out 20% of values
            mask = np.random.random(len(df)) < 0.2
            df.loc[mask, "value"] = None
            count = mask.sum()
            chaos_events.append({
                "type": "missing_values",
                "description": f"{count} values set to null",
                "severity": "high" if count > 10 else "medium"
            })
            logger.warning(f"CHAOS: Introduced {count} missing values")
        
        elif chaos_type == "duplicate_records":
            # Duplicate 10% of records
            sample = df.sample(frac=0.1)
            df = pd.concat([df, sample], ignore_index=True)
            chaos_events.append({
                "type": "duplicate_records",
                "description": f"{len(sample)} duplicate records introduced",
                "severity": "medium"
            })
            logger.warning(f"CHAOS: Introduced {len(sample)} duplicates")
        
        elif chaos_type == "schema_drift":
            # Rename a column to simulate API change
            df = df.rename(columns={"value": "val"})
            chaos_events.append({
                "type": "schema_drift",
                "description": "Column 'value' renamed to 'val' simulating API schema change",
                "severity": "critical"
            })
            logger.warning("CHAOS: Schema drift - column renamed")
        
        elif chaos_type == "late_data":
            # Remove last 5 records to simulate late arriving data
            df = df.iloc[:-5]
            chaos_events.append({
                "type": "late_data",
                "description": "Last 5 records missing - simulating late data arrival",
                "severity": "medium"
            })
            logger.warning("CHAOS: Removed last 5 records - late data simulation")
        
        elif chaos_type == "corrupted_values":
            # Replace some numeric values with strings
            idx = df.sample(frac=0.05).index
            df.loc[idx, "value"] = "ERROR"
            chaos_events.append({
                "type": "corrupted_values",
                "description": f"{len(idx)} values corrupted to string ERROR",
                "severity": "high"
            })
            logger.warning(f"CHAOS: Corrupted {len(idx)} values")
        
        elif chaos_type == "missing_series":
            # Drop all records for one series
            series_to_drop = random.choice(df["series_id"].unique().tolist())
            df = df[df["series_id"] != series_to_drop]
            chaos_events.append({
                "type": "missing_series",
                "description": f"All records for {series_to_drop} dropped - simulating source outage",
                "severity": "critical"
            })
            logger.warning(f"CHAOS: Dropped entire series {series_to_drop}")
    
    return df, chaos_events

if __name__ == "__main__":
    # Test chaos layer
    from ingestion import fetch_all_series
    
    df = fetch_all_series()
    print(f"Original shape: {df.shape}")
    print(f"Original null values: {df['value'].isnull().sum()}")
    
    corrupted_df, events = apply_chaos(df, chaos_probability=1.0)
    
    print(f"\nAfter chaos shape: {corrupted_df.shape}")
    print(f"\nChaos events applied:")
    for event in events:
        print(f"  - {event['type']}: {event['description']} (severity: {event['severity']})")