from models import db, EtlUser
from datetime import datetime

class UserService:
    def get_all_users(self):
        return EtlUser.query.order_by(EtlUser.created_at.desc()).all()

    def save_user(self, first_name, last_name, department):
        new_user = EtlUser(
            first_name=first_name,
            last_name=last_name,
            department=department,
            is_active=False
        )
        db.session.add(new_user)
        db.session.commit()
        return new_user

    def set_active_user(self, user_id):
        # Deactivate all
        EtlUser.query.update({EtlUser.is_active: False})
        
        # Activate one
        user = EtlUser.query.get(user_id)
        if user:
            user.is_active = True
            db.session.commit()
            return True
        return False

    def delete_user(self, user_id):
        user = EtlUser.query.get(user_id)
        if user:
            db.session.delete(user)
            db.session.commit()
            return True
        return False

    def get_active_user(self):
        return EtlUser.query.filter_by(is_active=True).first()
