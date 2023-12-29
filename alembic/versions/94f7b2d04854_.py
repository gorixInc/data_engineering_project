"""empty message

Revision ID: 94f7b2d04854
Revises: 7dfc03348500
Create Date: 2023-12-29 01:43:19.190180

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '94f7b2d04854'
down_revision = '7dfc03348500'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('authorship', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='dwh')
    op.add_column('category', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='dwh')
    op.add_column('journal', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='dwh')
    op.add_column('license', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='dwh')
    op.add_column('person', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='dwh')
    op.add_column('publication', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='dwh')
    op.add_column('publication_category', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='dwh')
    op.add_column('publication_journal', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='dwh')
    op.add_column('version', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='dwh')
    op.add_column('authorship', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='staging')
    op.add_column('category', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='staging')
    op.add_column('journal', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='staging')
    op.add_column('license', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='staging')
    op.add_column('person', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='staging')
    op.add_column('publication', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='staging')
    op.add_column('publication_category', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='staging')
    op.add_column('publication_journal', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='staging')
    op.add_column('sub_category', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='staging')
    op.add_column('version', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='staging')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('version', 'processed_at', schema='staging')
    op.drop_column('sub_category', 'processed_at', schema='staging')
    op.drop_column('publication_journal', 'processed_at', schema='staging')
    op.drop_column('publication_category', 'processed_at', schema='staging')
    op.drop_column('publication', 'processed_at', schema='staging')
    op.drop_column('person', 'processed_at', schema='staging')
    op.drop_column('license', 'processed_at', schema='staging')
    op.drop_column('journal', 'processed_at', schema='staging')
    op.drop_column('category', 'processed_at', schema='staging')
    op.drop_column('authorship', 'processed_at', schema='staging')
    op.drop_column('version', 'processed_at', schema='dwh')
    op.drop_column('publication_journal', 'processed_at', schema='dwh')
    op.drop_column('publication_category', 'processed_at', schema='dwh')
    op.drop_column('publication', 'processed_at', schema='dwh')
    op.drop_column('person', 'processed_at', schema='dwh')
    op.drop_column('license', 'processed_at', schema='dwh')
    op.drop_column('journal', 'processed_at', schema='dwh')
    op.drop_column('category', 'processed_at', schema='dwh')
    op.drop_column('authorship', 'processed_at', schema='dwh')
    # ### end Alembic commands ###