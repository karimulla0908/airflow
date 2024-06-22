import numpy as np
import matplotlib.pyplot as plt
import streamlit as st
from scipy import stats

np.random.seed(45)

# Load external CSS
with open("08 stylecssfile.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.markdown("<h1 class='title'>Sample Variance as an Unbiased Estimator of Population Variance</h1>", unsafe_allow_html=True)

st.sidebar.header("Parameters")

st.sidebar.markdown("<div class='section-title'>Population Parameters</div>", unsafe_allow_html=True)
population_size = st.sidebar.number_input("Population size", min_value=10, max_value=10000, value=20, step=10)
sample_size = st.sidebar.number_input("Sample size", min_value=10, max_value=10000, value=20, step=10)

ddof1_sample = st.sidebar.number_input("Degree of freedom", min_value=0, max_value=10, value=1, step=1)

st.sidebar.markdown("<div class='section-title'>Sampling Parameters</div>", unsafe_allow_html=True)
no_of_samples = st.sidebar.slider("Number of samples", min_value=1, max_value=100, value=10, step=1)
alpha = 0.05

calculate_button = st.sidebar.button("Calculate Variance of Samples")

def calculate_variance(population, sample_size, no_of_samples):
    samples_var = []
    pop_variance = np.var(population, ddof=0)
    for _ in range(no_of_samples):
        sample = np.random.choice(population, sample_size)
        sample_variance = np.var(sample, ddof=ddof1_sample)
        samples_var.append(sample_variance)
    return pop_variance, samples_var

if calculate_button:
    population = np.random.normal(loc=50, scale=2, size=population_size)
    pop_variance, samples_variance = calculate_variance(population, sample_size, no_of_samples)
    
    st.markdown(f"<h3 class='highlight'>Population variance: {pop_variance:.2f}</h3>", unsafe_allow_html=True)
    st.write("Sample variances:")
    
    sample_var_str = "<div class='sample-var-container'>"
    for i, var in enumerate(samples_variance, start=1):
        sample_var_str += f"<div class='sample-var'><strong>Sample {i}:</strong> {var:.2f}</div>"
    sample_var_str += "</div>"
    
    st.markdown(sample_var_str, unsafe_allow_html=True)
    
    # Perform hypothesis test
    sample_mean_var = np.mean(samples_variance)
    t_statistic, p_value = stats.ttest_1samp(samples_variance, pop_variance)
    
    st.markdown(f"<h2 class='result-text'>Average sample variance: {sample_mean_var:.2f}</h2>", unsafe_allow_html=True)
    st.markdown(f"<h2 class='result-text'>T-statistic: {t_statistic:.2f}</h2>", unsafe_allow_html=True)
    st.markdown(f"<h2 class='result-text'>P-value: {p_value:.2f}</h2>", unsafe_allow_html=True)
    
    # Calculate the confidence interval for the mean of sample variances
    confidence_level = 0.95
    degrees_freedom = ddof1_sample
    sample_var_std = np.std(samples_variance, ddof=1)
    sample_var_se = sample_var_std / np.sqrt(no_of_samples)
    confidence_interval = stats.t.interval(confidence_level, degrees_freedom, sample_mean_var, sample_var_se)
    
    st.markdown(f"<h2 class='result-text'>Confidence Interval for Average Sample Variance: ({confidence_interval[0]:.2f}, {confidence_interval[1]:.2f})</h2>", unsafe_allow_html=True)
    
    # Check if we reject the null hypothesis
    if p_value > alpha:
        st.markdown("<h2 class='result-text accept'>Accept the null hypothesis: The sample variance is an unbiased estimator of the population variance.</h2>", unsafe_allow_html=True)
    else:
        st.markdown("<h2 class='result-text reject'>Reject the null hypothesis: The sample variance is not an unbiased estimator of the population variance.</h2>", unsafe_allow_html=True)
    
    # Plotting the variances
    fig, ax = plt.subplots()
    ax.hist(samples_variance, bins='auto', alpha=0.7, label='Sample Variances', color='#3498db')
    ax.axvline(pop_variance, color='r', linestyle='dashed', linewidth=1, label='Population Variance')
    ax.axvline(sample_mean_var, color='g', linestyle='dashed', linewidth=1, label='Mean of Sample Variances')
    ax.legend()
    ax.set_title('Distribution of Sample Variances')
    ax.set_xlabel('Variance')
    ax.set_ylabel('Frequency')
    
    st.pyplot(fig)
