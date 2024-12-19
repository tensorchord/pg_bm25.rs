use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationErrors};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct IndexingOption {
    #[serde(default = "IndexingOption::default_partition")]
    pub partition: PartitionOption,
    #[serde(default = "IndexingOption::default_encode")]
    pub encode: EncodeOption,
}

impl IndexingOption {
    fn default_partition() -> PartitionOption {
        PartitionOption::Fixed
    }

    fn default_encode() -> EncodeOption {
        EncodeOption::DeltaBitpack
    }
}

impl Default for IndexingOption {
    fn default() -> Self {
        Self {
            partition: Self::default_partition(),
            encode: Self::default_encode(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum PartitionOption {
    Fixed,
    Variable(VariablePartitionOption),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct VariablePartitionOption {
    #[serde(default = "VariablePartitionOption::default_lambda")]
    #[validate(range(min = 0.0))]
    pub lambda: f32,
}

impl VariablePartitionOption {
    fn default_lambda() -> f32 {
        12.0
    }
}

impl Validate for PartitionOption {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            PartitionOption::Fixed => Ok(()),
            PartitionOption::Variable(options) => options.validate(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum EncodeOption {
    DeltaBitpack,
    EliasFano,
}
