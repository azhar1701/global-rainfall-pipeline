from typing import Dict, Any, List, Optional
import numpy as np
import pandas as pd
from scipy import stats

def calculate_trend(df: pd.DataFrame, column: str = 'precipitation', min_points: int = 10) -> Dict[str, Any]:
    """
    Calculates the statistical trend of a time series using Mann-Kendall (via Kendall Tau).
    Uses Theil-Sen estimator for the slope.
    """
    if len(df) < min_points:
        return {
            'status': 'insufficient_data',
            'trend': 'unknown',
            'p_value': None,
            'slope': 0.0
        }

    # Prepare data (clean NaNs and ensure numeric float type)
    valid_data = df[column].dropna().astype(float)
    if len(valid_data) < min_points:
        return {
            'status': 'insufficient_data',
            'trend': 'unknown',
            'p_value': None,
            'slope': 0.0
        }

    y = valid_data.values
    x = np.arange(len(y))

    # 1. Kendall Tau (Mann-Kendall equivalent)
    tau, p_value = stats.kendalltau(x, y)

    # 2. Theil-Sen Slope
    res = stats.theilslopes(y, x)
    slope = res[0]

    # Determine trend type
    if p_value < 0.05:
        trend = 'increasing' if slope > 0 else 'decreasing'
    else:
        trend = 'no_trend'

    return {
        'status': 'success',
        'trend': trend,
        'tau': float(tau),
        'p_value': float(p_value),
        'slope': float(slope),
        'intercept': float(res[1])
    }

def calculate_spi(df: pd.DataFrame, column: str = 'precipitation', window: int = 30) -> pd.DataFrame:
    """
    Calculates the Standardized Precipitation Index (SPI) using a Gamma distribution.
    A simple implementation for regional time-series data.
    """
    # Ensure precipitation is numeric and handle potential fitting errors more gracefully.
    # Convert the specified column to numeric, coercing errors to NaN, then fill NaNs with 0 for rolling sum.
    # Note: The original SPI calculation uses a rolling sum, so 0s are appropriate for missing data in this context.
    vals = pd.to_numeric(df[column], errors='coerce').fillna(0).values

    def fit_gamma_and_get_prob(x):
        # x is a numpy array representing the current window of precipitation values
        x_clean = x[~np.isnan(x)] # Remove NaNs from the window for fitting
        
        # Check for absolute minimum data requirement
        if len(x_clean) < 10:
            return np.nan

        # Traditional SPI handles zero-rainfall separately (Mixed Distribution)
        # q = probability of zero rain in the window
        zeros_mask = (x_clean == 0)
        q = np.mean(zeros_mask)
        
        # Current value to evaluate
        curr_val = x[-1]

        if q == 1.0:
            # Entire window is dry
            prob = 0.5 # Treat as normal for a dry region, or 1.0 for extreme dry? 
            # In SPI, if distribution is all zeros, it's "normal" dry until rain happens.
            # We use 0.5 to keep SPI at 0 (neutral).
            return 0.0
        
        try:
            x_nonzero = x_clean[~zeros_mask]
            
            # If we don't have enough non-zero data to fit a Gamma, fallback to empirical CDF
            if len(x_nonzero) < 5:
                prob = (np.sum(x_clean <= curr_val)) / len(x_clean)
            else:
                # Fit Gamma distribution to non-zero values
                shape, loc, scale = stats.gamma.fit(x_nonzero, floc=0)
                # G(x) is the cumulative probability from Gamma
                g_x = stats.gamma.cdf(curr_val, shape, loc, scale)
                # H(x) = q + (1-q)G(x) is the equiprobability transformation
                prob = q + (1.0 - q) * g_x
                
        except Exception:
            # Final fallback: statistical rank/empirical CFD
            prob = (np.sum(x_clean <= curr_val)) / len(x_clean)

        # Boundary control to avoid infinite SPI values (e.g. at probability 0 or 1)
        prob = max(0.001, min(0.999, prob))
        
        # Transform probability to high-fidelity Z-score
        return float(stats.norm.ppf(prob))

    # Apply the custom fitting function over a rolling window
    # raw=True passes numpy arrays to the function for performance
    spi_vals = pd.Series(vals, index=df.index).rolling(window=window, min_periods=10).apply(fit_gamma_and_get_prob, raw=True)
    
    # Add the calculated SPI values as a new column to the DataFrame
    df[f'spi_{window}'] = spi_vals
    return df

def calculate_ensemble_metrics(df: pd.DataFrame, col_a: str = 'precip_chirps', col_b: str = 'precip_gpm') -> Dict[str, Any]:
    """
    Production-grade Scientific Confidence Engine.
    
    Computes 5 independent accuracy metrics, each scored 0-1, then combines
    with hydrologically-meaningful weights into a composite confidence score.
    
    Metrics:
        1. RMSD — magnitude of disagreement
        2. Pearson R — temporal pattern agreement
        3. Bias Ratio — systematic over/under-estimation
        4. NSE (Nash-Sutcliffe Efficiency) — predictive skill
        5. Wet-Day Agreement (POD) — event detection consistency
    """
    if col_a not in df.columns or col_b not in df.columns:
        return {'status': 'error', 'msg': 'Missing provider columns for ensemble'}

    valid_df = df[[col_a, col_b]].dropna()
    n = len(valid_df)
    if n < 5:
        return {'status': 'insufficient_data'}

    a = valid_df[col_a].values.astype(float)
    b = valid_df[col_b].values.astype(float)

    # ── Metric 1: RMSD ──
    diff = a - b
    rmsd = float(np.sqrt(np.mean(diff**2)))
    avg_precip = float(np.mean(np.concatenate([a, b])))
    nrmsd = rmsd / avg_precip if avg_precip > 0.01 else 1.0
    rmsd_score = max(0.0, 1.0 - min(1.0, nrmsd))

    # ── Metric 2: Pearson Correlation ──
    if np.std(a) > 0 and np.std(b) > 0:
        corr, corr_pval = stats.pearsonr(a, b)
    else:
        corr, corr_pval = 0.0, 1.0
    corr = float(corr)
    corr_score = max(0.0, corr)

    # ── Metric 3: Bias Ratio ──
    sum_a, sum_b = float(np.sum(a)), float(np.sum(b))
    if sum_a > 0.01:
        bias_ratio = sum_b / sum_a
    elif sum_b > 0.01:
        bias_ratio = 2.0
    else:
        bias_ratio = 1.0
    bias_ratio = float(bias_ratio)
    bias_score = max(0.0, 1.0 - abs(bias_ratio - 1.0))

    # ── Metric 4: Nash-Sutcliffe Efficiency ──
    mean_a = float(np.mean(a))
    ss_res = float(np.sum((a - b)**2))
    ss_tot = float(np.sum((a - mean_a)**2))
    nse = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
    nse = float(nse)
    nse_score = max(0.0, min(1.0, nse))

    # ── Metric 5: Wet-Day Agreement (POD) ──
    wet_threshold = 1.0  # mm/day
    wet_a = a >= wet_threshold
    wet_b = b >= wet_threshold
    hits = int(np.sum(wet_a & wet_b))
    total_wet = int(np.sum(wet_a | wet_b))
    pod = hits / total_wet if total_wet > 0 else 1.0
    pod = float(pod)

    # ── Composite Weighted Score ──
    weights = {'correlation': 0.30, 'nse': 0.25, 'rmsd': 0.20, 'bias': 0.15, 'pod': 0.10}
    composite = float(round(
        weights['correlation'] * corr_score +
        weights['nse'] * nse_score +
        weights['rmsd'] * rmsd_score +
        weights['bias'] * bias_score +
        weights['pod'] * pod
    , 3))

    # ── Grade Assignment ──
    if composite >= 0.85:
        grade, confidence = 'A', 'High'
    elif composite >= 0.70:
        grade, confidence = 'B', 'Good'
    elif composite >= 0.50:
        grade, confidence = 'C', 'Medium'
    elif composite >= 0.30:
        grade, confidence = 'D', 'Low'
    else:
        grade, confidence = 'F', 'Very Low'

    # ── Dynamic Explainer ──
    explainer = _build_confidence_explainer(
        confidence, grade, composite, corr, rmsd, bias_ratio, nse, pod, n
    )

    return {
        'status': 'success',
        'scientific_confidence': confidence,
        'grade': grade,
        'composite_score': composite,
        'n_days': n,
        'metrics': {
            'rmsd': {'value': round(rmsd, 3), 'score': round(rmsd_score, 3), 'unit': 'mm/day'},
            'correlation': {'value': round(corr, 3), 'p_value': round(float(corr_pval), 4), 'score': round(corr_score, 3)},
            'bias_ratio': {'value': round(bias_ratio, 3), 'score': round(bias_score, 3)},
            'nse': {'value': round(nse, 3), 'score': round(nse_score, 3)},
            'pod': {'value': round(pod, 3), 'wet_days': total_wet}
        },
        'weights': weights,
        'explainer': explainer,
        # Legacy compatibility
        'rmsd': float(rmsd),
        'correlation': float(corr),
        'consistency_score': float(composite)
    }


def _build_confidence_explainer(
    confidence: str, grade: str, composite: float,
    corr: float, rmsd: float, bias_ratio: float,
    nse: float, pod: float, n_days: int
) -> str:
    """Generates a human-readable scientific interpretation."""
    parts = []
    parts.append(f"Grade {grade} ({confidence}) | Composite: {composite:.0%} | {n_days} observation days")

    # Correlation
    if corr >= 0.9:
        parts.append(f"R={corr:.2f}: Excellent temporal sync — both sensors agree on rainfall timing.")
    elif corr >= 0.7:
        parts.append(f"R={corr:.2f}: Good pattern match with minor day-to-day deviations.")
    elif corr >= 0.5:
        parts.append(f"R={corr:.2f}: Moderate — some events detected differently between sensors.")
    else:
        parts.append(f"R={corr:.2f}: Weak — significant temporal disagreement. Consider a single source.")

    # RMSD
    if rmsd < 3:
        parts.append(f"RMSD={rmsd:.1f}mm: Very low — datasets nearly interchangeable here.")
    elif rmsd < 8:
        parts.append(f"RMSD={rmsd:.1f}mm: Moderate — acceptable for regional analysis.")
    else:
        parts.append(f"RMSD={rmsd:.1f}mm: High — likely orographic effects or sensor gaps.")

    # Bias
    if 0.9 <= bias_ratio <= 1.1:
        parts.append(f"Bias={bias_ratio:.2f}: No systematic over/under-estimation.")
    elif bias_ratio > 1.1:
        parts.append(f"Bias={bias_ratio:.2f}: GPM is {((bias_ratio-1)*100):.0f}% wetter than CHIRPS.")
    else:
        parts.append(f"Bias={bias_ratio:.2f}: GPM is {((1-bias_ratio)*100):.0f}% drier than CHIRPS.")

    # NSE
    if nse >= 0.75:
        parts.append(f"NSE={nse:.2f}: Strong predictive skill.")
    elif nse >= 0.36:
        parts.append(f"NSE={nse:.2f}: Satisfactory for trend detection.")
    else:
        parts.append(f"NSE={nse:.2f}: Below threshold — mean is a better predictor.")

    # POD
    if pod >= 0.8:
        parts.append(f"Wet-Day Match={pod:.0%}: Strong event agreement.")
    elif pod >= 0.5:
        parts.append(f"Wet-Day Match={pod:.0%}: Some rain days missed by one dataset.")
    else:
        parts.append(f"Wet-Day Match={pod:.0%}: Poor — datasets disagree on wet/dry days.")

    return " | ".join(parts)
