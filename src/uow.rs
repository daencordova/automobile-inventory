use std::ops::{Deref, DerefMut};

use sqlx::{PgConnection, Postgres, Transaction};

use crate::error::AppError;

pub struct UnitOfWork<'a> {
    tx: Option<Transaction<'a, Postgres>>,
    committed: bool,
    rolled_back: bool,
}

impl<'a> UnitOfWork<'a> {
    pub async fn begin(pool: &sqlx::PgPool) -> Result<Self, AppError> {
        let tx = pool.begin().await.map_err(|e| AppError::DatabaseError(e))?;

        Ok(Self {
            tx: Some(tx),
            committed: false,
            rolled_back: false,
        })
    }

    pub async fn commit(mut self) -> Result<(), AppError> {
        if self.committed || self.rolled_back {
            return Err(AppError::BusinessRuleViolation(
                "Transaction already finalized".to_string(),
            ));
        }

        let tx = self.tx.take().ok_or_else(|| {
            AppError::BusinessRuleViolation("Transaction already consumed".to_string())
        })?;

        tx.commit().await.map_err(|e| AppError::DatabaseError(e))?;

        self.committed = true;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), AppError> {
        if self.committed || self.rolled_back {
            return Err(AppError::BusinessRuleViolation(
                "Transaction already finalized".to_string(),
            ));
        }

        let tx = self.tx.take().ok_or_else(|| {
            AppError::BusinessRuleViolation("Transaction already consumed".to_string())
        })?;

        tx.rollback()
            .await
            .map_err(|e| AppError::DatabaseError(e))?;

        self.rolled_back = true;
        Ok(())
    }

    pub fn connection(&mut self) -> &mut PgConnection {
        self.tx.as_mut().unwrap()
    }

    pub fn is_active(&self) -> bool {
        !self.committed && !self.rolled_back && self.tx.is_some()
    }
}

impl<'a> Deref for UnitOfWork<'a> {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        self.tx.as_ref().unwrap()
    }
}

impl<'a> DerefMut for UnitOfWork<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.tx.as_mut().unwrap()
    }
}

impl<'a> Drop for UnitOfWork<'a> {
    fn drop(&mut self) {
        if self.tx.is_some() && !std::thread::panicking() {
            tracing::warn!(
                "UnitOfWork dropped without explicit commit/rollback - potential data inconsistency. \
                Consider using explicit commit() or rollback()."
            );
        }
    }
}

#[async_trait::async_trait]
pub trait UnitOfWorkFactory: Send + Sync {
    async fn create_uow(&self) -> Result<UnitOfWork<'_>, AppError>;
}

pub struct PgUnitOfWorkFactory {
    pool: sqlx::PgPool,
}

impl PgUnitOfWorkFactory {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl UnitOfWorkFactory for PgUnitOfWorkFactory {
    async fn create_uow(&self) -> Result<UnitOfWork<'_>, AppError> {
        UnitOfWork::begin(&self.pool).await
    }
}
