"""Visualization utilities for plotting traffic and air quality data."""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
from pathlib import Path
import logging

from config import PLOTS_DIR, PM25_THRESHOLD, SPEED_THRESHOLD

logger = logging.getLogger(__name__)


def plot_time_series(combined_df, output_file=None, show_critical=True):
    """
    Create a time series plot showing average speed and PM2.5 over time.
    
    Args:
        combined_df: DataFrame with columns: timestamp, avg_speed, avg_pm25
        output_file: Optional output file path
        show_critical: Whether to highlight critical periods
    """
    if combined_df.empty:
        logger.warning("Empty DataFrame, cannot create plot")
        return
    
    if "timestamp" not in combined_df.columns:
        logger.error("DataFrame missing 'timestamp' column")
        return
    
    # Ensure timestamp column is datetime and timezone-aware
    df = combined_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    # If timezone-aware, convert to naive for plotting (matplotlib handles timezone-aware poorly)
    # But keep the original timezone information for display
    if df["timestamp"].dt.tz is not None:
        # Store timezone for reference
        tz = df["timestamp"].dt.tz
        # Convert to naive datetime (local time) for plotting
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    
    # Plot average speed
    if "avg_speed" in df.columns:
        ax1.plot(df["timestamp"], df["avg_speed"], 
                label="Average Speed", color="blue", linewidth=1.5)
        ax1.axhline(y=SPEED_THRESHOLD, color="red", linestyle="--", 
                   label=f"Speed Threshold ({SPEED_THRESHOLD} km/h)", alpha=0.7)
        ax1.set_ylabel("Average Speed (km/h)", fontsize=12)
        ax1.set_title("Traffic Speed and Air Quality Over Time", fontsize=14, fontweight="bold")
        ax1.legend(loc="best")
        ax1.grid(True, alpha=0.3)
    
    # Plot PM2.5
    if "avg_pm25" in df.columns:
        ax2.plot(df["timestamp"], df["avg_pm25"], 
                label="PM2.5", color="orange", linewidth=1.5)
        ax2.axhline(y=PM25_THRESHOLD, color="red", linestyle="--", 
                   label=f"PM2.5 Threshold ({PM25_THRESHOLD} µg/m³)", alpha=0.7)
        ax2.set_ylabel("PM2.5 (µg/m³)", fontsize=12)
        ax2.set_xlabel("Time", fontsize=12)
        ax2.legend(loc="best")
        ax2.grid(True, alpha=0.3)
    
    # Highlight critical periods
    if show_critical and "is_critical" in df.columns:
        critical_periods = df[df["is_critical"] == True]
        if not critical_periods.empty:
            for ax in [ax1, ax2]:
                for _, row in critical_periods.iterrows():
                    ax.axvline(x=row["timestamp"], color="red", alpha=0.3, linewidth=2)
    
    # Format x-axis dates
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    ax2.xaxis.set_major_locator(mdates.HourLocator(interval=max(1, len(df) // 20)))
    plt.xticks(rotation=45, ha="right")
    
    plt.tight_layout()
    
    # Save plot
    if output_file is None:
        output_file = PLOTS_DIR / "time_series.png"
    else:
        output_file = Path(output_file)
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    logger.info(f"Time series plot saved to {output_file}")
    
    plt.close()


def plot_scatter(combined_df, output_file=None, show_critical=True):
    """
    Create a scatter plot of PM2.5 vs average speed.
    
    Args:
        combined_df: DataFrame with columns: avg_speed, avg_pm25
        output_file: Optional output file path
        show_critical: Whether to highlight critical points
    """
    if combined_df.empty:
        logger.warning("Empty DataFrame, cannot create plot")
        return
    
    required_cols = ["avg_speed", "avg_pm25"]
    if not all(col in combined_df.columns for col in required_cols):
        logger.error(f"DataFrame missing required columns: {required_cols}")
        return
    
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Filter out NaN values
    plot_df = combined_df.dropna(subset=required_cols)
    
    if plot_df.empty:
        logger.warning("No valid data points for scatter plot")
        return
    
    # Plot all points
    ax.scatter(plot_df["avg_speed"], plot_df["avg_pm25"], 
              alpha=0.6, s=50, color="gray", label="All Data Points")
    
    # Highlight critical periods
    if show_critical and "is_critical" in plot_df.columns:
        critical_df = plot_df[plot_df["is_critical"] == True]
        if not critical_df.empty:
            ax.scatter(critical_df["avg_speed"], critical_df["avg_pm25"], 
                      alpha=0.8, s=100, color="red", 
                      edgecolors="darkred", linewidth=2, 
                      label="Critical Periods", zorder=5)
    
    # Add threshold lines
    ax.axhline(y=PM25_THRESHOLD, color="red", linestyle="--", 
              label=f"PM2.5 Threshold ({PM25_THRESHOLD} µg/m³)", alpha=0.7)
    ax.axvline(x=SPEED_THRESHOLD, color="blue", linestyle="--", 
              label=f"Speed Threshold ({SPEED_THRESHOLD} km/h)", alpha=0.7)
    
    # Shade critical region
    ax.axhspan(PM25_THRESHOLD, ax.get_ylim()[1], alpha=0.1, color="red")
    ax.axvspan(0, SPEED_THRESHOLD, alpha=0.1, color="blue")
    
    ax.set_xlabel("Average Speed (km/h)", fontsize=12)
    ax.set_ylabel("PM2.5 (µg/m³)", fontsize=12)
    ax.set_title("PM2.5 vs Traffic Speed", fontsize=14, fontweight="bold")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save plot
    if output_file is None:
        output_file = PLOTS_DIR / "scatter_plot.png"
    else:
        output_file = Path(output_file)
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    logger.info(f"Scatter plot saved to {output_file}")
    
    plt.close()


def plot_critical_periods_timeline(critical_df, output_file=None):
    """
    Create a timeline visualization of critical periods with detailed information.
    
    Args:
        critical_df: DataFrame with critical periods (must have timestamp column)
        output_file: Optional output file path
    """
    if critical_df.empty:
        logger.warning("No critical periods to plot")
        return
    
    if "timestamp" not in critical_df.columns:
        logger.error("DataFrame missing 'timestamp' column")
        return
    
    # Ensure timestamp column is datetime and timezone-aware
    df = critical_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    # If timezone-aware, convert to naive for plotting (matplotlib handles timezone-aware poorly)
    if df["timestamp"].dt.tz is not None:
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    
    # Sort by timestamp
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # Determine color intensity based on severity index if available
    if "severity_index" in df.columns and len(df) > 0:
        # Normalize severity index for color mapping (0-1 scale)
        severity_norm = df["severity_index"].values
        max_severity = severity_norm.max()
        if max_severity > 0:
            # Use colormap from light red to dark red
            normalized = 0.4 + 0.6 * (severity_norm / max_severity)
            colors = [plt.cm.Reds(val) for val in normalized]
        else:
            colors = ["#d32f2f"] * len(df)  # Default dark red
    else:
        colors = ["#d32f2f"] * len(df)  # Default dark red
    
    # Create timeline bars with labels
    bar_height = 0.6
    y_pos = 0
    
    for idx, row in df.iterrows():
        # Create bar
        bar_color = colors[idx] if idx < len(colors) else "#d32f2f"
        bar = ax.barh(y_pos, width=pd.Timedelta(hours=1), left=row["timestamp"], 
                     height=bar_height, color=bar_color, 
                     alpha=0.8, edgecolor="darkred", linewidth=1.5)
        
        # Add text labels with key information
        center_time = row["timestamp"] + pd.Timedelta(hours=0.5)
        
        # Format time label
        time_label = row["timestamp"].strftime("%H:%M")
        
        # Build info text
        info_parts = []
        if "avg_pm25" in df.columns and pd.notna(row["avg_pm25"]):
            info_parts.append(f"PM2.5: {row['avg_pm25']:.1f} µg/m³")
        if "avg_speed" in df.columns and pd.notna(row["avg_speed"]):
            info_parts.append(f"Speed: {row['avg_speed']:.1f} km/h")
        
        info_text = "\n".join(info_parts)
        
        # Add time label at the top of the bar
        ax.text(center_time, y_pos + bar_height/2 + 0.15, time_label,
               ha="center", va="bottom", fontsize=10, fontweight="bold", color="darkred")
        
        # Add info text in the center of the bar
        if info_text:
            ax.text(center_time, y_pos, info_text,
                   ha="center", va="center", fontsize=9, color="white", 
                   fontweight="bold", bbox=dict(boxstyle="round,pad=0.3", 
                   facecolor="black", alpha=0.6, edgecolor="white", linewidth=1))
    
    # Set labels and title
    ax.set_xlabel("Time", fontsize=12, fontweight="bold")
    ax.set_ylabel("Critical Periods", fontsize=12, fontweight="bold")
    ax.set_title("Timeline of Critical Periods (High PM2.5 and Low Speed)", 
                fontsize=16, fontweight="bold", pad=20)
    
    # Set y-axis
    ax.set_ylim(-0.3, 0.9)
    ax.set_yticks([y_pos])
    ax.set_yticklabels([""])
    
    # Format x-axis with better spacing
    if len(df) > 0:
        time_range = df["timestamp"].max() - df["timestamp"].min()
        if time_range <= pd.Timedelta(hours=24):
            # Show hourly ticks if within 24 hours
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
            ax.xaxis.set_minor_locator(mdates.MinuteLocator(interval=30))
        else:
            # Show daily ticks if longer
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
            ax.xaxis.set_minor_locator(mdates.HourLocator(interval=6))
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    plt.xticks(rotation=45, ha="right")
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, linestyle="--", axis="x")
    ax.set_axisbelow(True)
    
    # Add summary statistics as text box
    if len(df) > 0:
        stats_text = f"Total Critical Periods: {len(df)}\n"
        if "avg_pm25" in df.columns and df["avg_pm25"].notna().any():
            avg_pm25 = df["avg_pm25"].mean()
            max_pm25 = df["avg_pm25"].max()
            stats_text += f"Avg PM2.5: {avg_pm25:.1f} µg/m³ (Max: {max_pm25:.1f})\n"
        if "avg_speed" in df.columns and df["avg_speed"].notna().any():
            avg_speed = df["avg_speed"].mean()
            min_speed = df["avg_speed"].min()
            stats_text += f"Avg Speed: {avg_speed:.1f} km/h (Min: {min_speed:.1f})"
        
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
               fontsize=10, verticalalignment="top",
               bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8, edgecolor="black"))
    
    # Add legend for severity if available
    if "severity_index" in df.columns and len(df) > 1:
        # Create a colorbar to show severity scale
        sm = plt.cm.ScalarMappable(cmap=plt.cm.Reds, 
                                  norm=plt.Normalize(vmin=df["severity_index"].min(), 
                                                    vmax=df["severity_index"].max()))
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax, orientation="horizontal", pad=0.1, aspect=40)
        cbar.set_label("Severity Index", fontsize=10, fontweight="bold")
    
    plt.tight_layout()
    
    # Save plot
    if output_file is None:
        output_file = PLOTS_DIR / "critical_periods_timeline.png"
    else:
        output_file = Path(output_file)
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    logger.info(f"Critical periods timeline saved to {output_file}")
    
    plt.close()


def create_all_plots(combined_df, critical_df=None):
    """
    Create all visualization plots.
    
    Args:
        combined_df: Combined DataFrame with traffic and air quality data
        critical_df: Optional DataFrame with critical periods
    """
    logger.info("Creating visualization plots...")
    
    # Time series plot
    plot_time_series(combined_df, show_critical=(critical_df is not None and not critical_df.empty))
    
    # Scatter plot
    plot_scatter(combined_df, show_critical=(critical_df is not None and not critical_df.empty))
    
    # Critical periods timeline
    if critical_df is not None and not critical_df.empty:
        plot_critical_periods_timeline(critical_df)
    
    logger.info("All plots created successfully")

