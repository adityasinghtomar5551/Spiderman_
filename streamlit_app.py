import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, roc_auc_score
import io

# --- Page Configuration ---
st.set_page_config(
    page_title="Titanic Survival Predictor",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Helper Functions (Caching for performance) ---
@st.cache_data
def load_data(train_file, test_file):
    """Loads and caches the training and testing data."""
    try:
        train_df = pd.read_csv(train_file)
        test_df = pd.read_csv(test_file)
        return train_df, test_df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None

@st.cache_data
def preprocess_data(train_df, test_df):
    """Applies all preprocessing steps to the data."""
    train_processed = train_df.copy()
    test_processed = test_df.copy()
    
    age_mean = train_processed['Age'].mean()
    train_processed['Age'].fillna(age_mean, inplace=True)
    test_processed['Age'].fillna(age_mean, inplace=True)
    
    fare_median = train_processed['Fare'].median()
    train_processed['Fare'].fillna(fare_median, inplace=True)
    test_processed['Fare'].fillna(fare_median, inplace=True)
    
    train_processed['Sex'] = train_processed['Sex'].replace(['female', 'male'], [0, 1])
    test_processed['Sex'] = test_processed['Sex'].replace(['female', 'male'], [0, 1])
    
    cols_to_drop = ['Name', 'Ticket', 'Cabin', 'Embarked']
    train_processed.drop(columns=cols_to_drop, inplace=True)
    test_processed.drop(columns=cols_to_drop, inplace=True)
    
    return train_processed, test_processed

@st.cache_data
def train_model(train_processed):
    """Trains the Logistic Regression model and splits the data."""
    X = train_processed.drop(columns=['Survived', 'PassengerId'])
    y = train_processed['Survived']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=42)
    
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)
    
    return model, X_test, y_test

@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# --- Sidebar ---
with st.sidebar:
    st.title("🚢 Titanic Survival Predictor")
    
    # Notebook Image
    st.image("Screenshot_25z0.png")
    
    st.header("1. Upload Your Data")
    st.write("Please upload the `train.csv` and `test.csv` files.")
    
    uploaded_train_file = st.file_uploader("Upload train.csv", type="csv")
    uploaded_test_file = st.file_uploader("Upload test.csv", type="csv")
    
    st.info("[Download the data from Kaggle](https://www.kaggle.com/competitions/titanic/data)")

# --- Main Application ---
if uploaded_train_file is not None and uploaded_test_file is not None:
    train_data, test_data = load_data(uploaded_train_file, uploaded_test_file)
    train_processed, test_processed = preprocess_data(train_data, test_data)
    model, X_test, y_test = train_model(train_processed)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Problem & EDA", 
        "⚙️ Data Preprocessing", 
        "🧠 Model Training & Prediction", 
        "📈 Model Evaluation", 
        "📋 Conclusion"
    ])

    # --- Tab 1: Problem Definition & EDA ---
    with tab1:
        st.header("Problem Definition")
        st.image("Screenshot_245.png")
        
        st.header("Source")
        st.image("Screenshot_253.png")

        st.image("Screenshot_254.png")
        st.header("Exploratory Data Analysis (EDA)")
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Raw Training Data")
            st.dataframe(train_data.head(), height=210)
            
            st.subheader("Data Shapes")
            st.code(f"Training Data Shape: {train_data.shape}\nTesting Data Shape:  {test_data.shape}")

        with col2:
            st.subheader("Raw Test Data")
            st.dataframe(test_data.head(), height=210)
            
            st.subheader("Missing Values (Train)")
            missing_train = train_data.isnull().sum()
            st.dataframe(missing_train[missing_train > 0], height=120)
            
            st.subheader("Missing Values (Test)")
            missing_test = test_data.isnull().sum()
            st.dataframe(missing_test[missing_test > 0], height=120)

        st.image("Screenshot_254.png")
        st.subheader("Correlation of Numeric Features (Raw Data)")
        
        eda_corr = train_data[['Survived', 'Pclass', 'Age', 'SibSp', 'Parch', 'Fare']].corr()
        fig_eda_corr, ax_eda_corr = plt.subplots(figsize=(10, 6))
        sns.heatmap(eda_corr, annot=True, ax=ax_eda_corr, cmap='viridis', fmt='.2f')
        ax_eda_corr.set_title("EDA Correlation Heatmap")
        st.pyplot(fig_eda_corr)

    # --- Tab 2: Data Preprocessing ---
    with tab2:
        st.header("Data Preprocessing")
        st.image("Screenshot_253.png")
        st.subheader("Handling Missing Values")
        st.markdown("""
        - **Age**: Imputed with the mean age from the training set.
        - **Fare**: Imputed with the median fare from the training set.
        """)
        
        st.image("Screenshot_248.png") # Image from notebook
        
        st.image("Screenshot_254.png")
        st.subheader("Converting Categorical Data")
        st.markdown("- **Sex**: Mapped 'female' to `0` and 'male' to `1`.")
        
        st.image("Screenshot_254.png")
        st.subheader("Dropping Unnecessary Columns")
        st.markdown("- Removed 'Name', 'Ticket', 'Cabin', and 'Embarked'.")
        
        st.image("Screenshot_254.png")
        st.subheader("Data After Preprocessing")
        st.dataframe(train_processed.head())
        
        st.image("Screenshot_254.png")
        st.subheader("Correlation After Preprocessing")
        st.write("This heatmap shows the correlations *after* cleaning and converting data. 'Sex' (now numeric) shows the strongest correlation with 'Survived'.")
        
        X_corr = train_processed.drop(columns=['Survived', 'PassengerId'])
        corr = X_corr.corr()
        
        fig_corr, ax_corr = plt.subplots(figsize=(10, 6))
        sns.heatmap(corr, annot=True, ax=ax_corr, cmap='coolwarm', fmt='.2f')
        ax_corr.set_title("Post-Preprocessing Correlation Heatmap")
        st.pyplot(fig_corr)

    # --- Tab 3: Model Training & Prediction ---
    with tab3:
        st.header("Design Architecture")
        st.image("Screenshot_244.png")
        
        st.image("Screenshot_253.png")
        st.header("Model Training: Logistic Regression")
        st.write("A Logistic Regression model is trained on the preprocessed training dataset.")
        
        st.code(f"""
# Features (X)
{list(X_test.columns)}

# Target (y)
['Survived']

# Model
model = LogisticRegression(max_iter=1000)
model.fit(X_train_full, y_train_full)
        """)
        st.success("Model trained successfully!")
        
        st.image("Screenshot_254.png")
        st.header("Generate Predictions on Test Data")
        st.write("The trained model is now used to predict survival for the `test.csv` data.")
        
        X_final_test = test_processed.drop(columns=['PassengerId'])
        predictions = model.predict(X_final_test)
        
        submission_df = pd.DataFrame({
            'PassengerId': test_processed['PassengerId'], 
            'Survived': predictions
        })
        
        st.dataframe(submission_df.head())
        
        csv_download = convert_df_to_csv(submission_df)
        st.download_button(
            label="📥 Download Predictions as CSV",
            data=csv_download,
            file_name="titanic_survivors.csv",
            mime="text/csv",
        )

    # --- Tab 4: Model Evaluation ---
    with tab4:
        st.header("Evaluate the Model")
        st.image("Screenshot_253.png")
        st.write("These metrics are calculated from a 70/30 train-test split of the *training data*.")
        
        y_pred = model.predict(X_test)
        
        # Calculate metrics
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)
        roc_auc = roc_auc_score(y_test, y_pred)
        
        st.subheader("Performance Metrics")
        st.image("Screenshot_254.png")
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Accuracy", f"{accuracy:.2%}")
        col2.metric("Precision", f"{precision:.2%}")
        col3.metric("Recall", f"{recall:.2%}")
        col1.metric("F1 Score", f"{f1:.2f}")
        col2.metric("ROC-AUC Score", f"{roc_auc:.2f}")
        
        st.image("Screenshot_254.png")
        st.subheader("Confusion Matrix")
        
        # Display the confusion matrix image from your notebook
        st.image("confusion_matrix.png")
        
        # Display the seaborn heatmap
        conf_matrix = confusion_matrix(y_test, y_pred)
        fig_cm, ax_cm = plt.subplots(figsize=(8, 6))
        sns.heatmap(conf_matrix/np.sum(conf_matrix), annot=True, fmt='.2%', cmap='Blues', ax=ax_cm)
        ax_cm.set_xlabel("Predicted Label")
        ax_cm.set_ylabel("True Label")
        ax_cm.set_title("Confusion Matrix (Percentages)")
        st.pyplot(fig_cm)
        

    # --- Tab 5: Conclusion ---
    with tab5:
        st.header("Interpretation of Results")
        
        # Notebook Image
        st.image("Screenshot_252.png")
        
        # --- THIS IS THE FIXED SECTION ---
        # Call st.markdown and st.image separately
        
        st.markdown(f"""
        ### Accuracy Score: {accuracy:.2%}
        * This indicates that the model correctly predicts whether a passenger survived or not **{accuracy:.2%}** of the time. It's a good measure of the model's overall performance.
        """)
        
        st.image("Screenshot_254.png", width=300)
        
        st.markdown(f"""
        ### Precision Score: {precision:.2%}
        * Precision measures the accuracy of *positive* predictions. When the model predicts a passenger survived, it is correct **{precision:.2%}** of the time.
        """)
        
        st.image("Screenshot_254.png", width=300)
        
        st.markdown(f"""
        ### Recall Score: {recall:.2%}
        * Recall measures the model's ability to find all actual survivors. The model correctly identifies **{recall:.2%}** of all people who *actually* survived.
        """)
        
        st.image("Screenshot_254.png", width=300)

        st.markdown(f"""
        ### F1 Score: {f1:.2f}
        * The F1 Score is the weighted average of precision and recall. At **{f1:.2f}**, it suggests a good balance between precision and recall.
        """)
        
        st.image("Screenshot_254.png", width=300)
        
        conf_matrix = confusion_matrix(y_test, y_pred) # Recalculate just in case for this tab
        st.markdown(f"""
        ### Confusion Matrix Breakdown
        * **True Negatives (TN):** {conf_matrix[0, 0]} (Correctly predicted non-survivors)
        * **False Positives (FP):** {conf_matrix[0, 1]} (Incorrectly predicted as survivors)
        * **False Negatives (FN):** {conf_matrix[1, 0]} (Incorrectly predicted as non-survivors)
        * **True Positives (TP):** {conf_matrix[1, 1]} (Correctly predicted survivors)
        """)
        
        st.image("Screenshot_253.png", width=300)
        
        st.markdown("""
        ### Overall
        The logistic regression model shows a strong ability to predict survival, with high accuracy and precision.
        """)
        
        st.image("Screenshot_250FF.png")
        # --- END OF FIXED SECTION ---

else:
    st.header("Welcome to the Titanic Survival Prediction App 🚢")
    st.subheader("Please upload the `train.csv` and `test.csv` files in the sidebar to begin.")
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/f/fd/RMS_Titanic_3.jpg/1280px-RMS_Titanic_3.jpg", 
             caption="RMS Titanic departing from Southampton on April 10, 1912.")
