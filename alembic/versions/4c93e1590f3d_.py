"""empty message

Revision ID: 4c93e1590f3d
Revises: 94f7b2d04854
Create Date: 2023-12-29 01:47:05.033049

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4c93e1590f3d'
down_revision = '94f7b2d04854'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('sub_category', sa.Column('processed_at', sa.DateTime(), nullable=True), schema='dwh')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('sub_category', 'processed_at', schema='dwh')
    # ### end Alembic commands ###
