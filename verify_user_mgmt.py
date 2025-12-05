from app import app, db
from models import EtlUser
from services.user_service import UserService

with app.app_context():
    # Create tables if not exist
    db.create_all()
    print("Tables created.")

    service = UserService()
    
    # Test Create
    print("Creating user...")
    user = service.save_user("John", "Doe", "Engineering")
    print(f"User created: {user.first_name} {user.last_name} (ID: {user.id})")
    
    # Test Get All
    users = service.get_all_users()
    print(f"Total users: {len(users)}")
    
    # Test Activate
    print("Activating user...")
    service.set_active_user(user.id)
    active_user = service.get_active_user()
    print(f"Active user: {active_user.first_name} (ID: {active_user.id})")
    
    # Test Delete
    print("Deleting user...")
    service.delete_user(user.id)
    users_after = service.get_all_users()
    print(f"Total users after delete: {len(users_after)}")
